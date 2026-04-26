/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

// fakeNotifier records all calls made to it.
type fakeNotifier struct {
	mu      sync.Mutex
	upserts []*fwkdl.EndpointMetadata
	deletes []types.NamespacedName
	synced  bool
}

func (n *fakeNotifier) Upsert(meta *fwkdl.EndpointMetadata) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.upserts = append(n.upserts, meta)
}

func (n *fakeNotifier) Delete(id types.NamespacedName) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deletes = append(n.deletes, id)
}

func (n *fakeNotifier) MarkSynced() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.synced = true
}

func (n *fakeNotifier) upsertCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.upserts)
}

// writeTempFile writes content to a new temp file and returns its path.
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "backends.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

const sampleYAML = `
backends:
  - name: vllm-0
    namespace: ns1
    address: "10.0.0.1"
    port: "8000"
    metricsHost: "10.0.0.1:9090"
    labels:
      model: llama3
  - name: vllm-1
    namespace: ns1
    address: "10.0.0.2"
    port: "8000"
`

// TestFactory validates parameter parsing and error cases.
func TestFactory(t *testing.T) {
	tests := []struct {
		name      string
		params    map[string]any
		wantErr   bool
		wantWatch bool
	}{
		{
			name:    "missing path returns error",
			params:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid params with watchFile false",
			params:  map[string]any{"path": "/tmp/backends.yaml", "watchFile": false},
			wantErr: false,
		},
		{
			name:      "valid params with watchFile true",
			params:    map[string]any{"path": "/tmp/backends.yaml", "watchFile": true},
			wantErr:   false,
			wantWatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(tt.params)
			require.NoError(t, err)

			plugin, err := Factory("test", raw, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			fd := plugin.(*FileDiscovery)
			assert.Equal(t, PluginType, fd.TypedName().Type)
			assert.Equal(t, tt.wantWatch, fd.watchFile)
		})
	}
}

// TestLoad_AddAndDelete verifies that successive load() calls correctly upsert new
// backends and delete backends that are no longer in the file.
func TestLoad_AddAndDelete(t *testing.T) {
	first := `
backends:
  - name: keep
    namespace: ns
    address: "10.0.0.1"
    port: "8000"
  - name: remove
    namespace: ns
    address: "10.0.0.2"
    port: "8000"
`
	second := `
backends:
  - name: keep
    namespace: ns
    address: "10.0.0.1"
    port: "8000"
  - name: added
    namespace: ns
    address: "10.0.0.3"
    port: "8000"
`
	path := writeTempFile(t, first)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
	}
	notifier := &fakeNotifier{}

	// First load: both backends upserted, nothing deleted.
	require.NoError(t, fd.load(context.Background(), notifier))
	assert.Len(t, notifier.upserts, 2)
	assert.Empty(t, notifier.deletes)

	// Second load: "added" upserted, "remove" deleted, "keep" re-upserted.
	require.NoError(t, os.WriteFile(path, []byte(second), 0o644))
	require.NoError(t, fd.load(context.Background(), notifier))

	assert.Len(t, notifier.upserts, 4) // 2 from first load + 2 from second
	require.Len(t, notifier.deletes, 1)
	assert.Equal(t, "remove", notifier.deletes[0].Name)
	assert.Equal(t, "ns", notifier.deletes[0].Namespace)
}

// TestLoad_DeleteAll verifies that when the file becomes empty all previous backends are deleted.
func TestLoad_DeleteAll(t *testing.T) {
	path := writeTempFile(t, sampleYAML)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
	}
	notifier := &fakeNotifier{}

	require.NoError(t, fd.load(context.Background(), notifier))
	assert.Len(t, notifier.upserts, 2)

	require.NoError(t, os.WriteFile(path, []byte("backends: []\n"), 0o644))
	require.NoError(t, fd.load(context.Background(), notifier))

	assert.Len(t, notifier.deletes, 2)
	assert.Len(t, notifier.upserts, 2) // no new upserts on second load
}

// TestLoad_ValidYAML verifies that a valid backends YAML file is loaded correctly.
func TestLoad_ValidYAML(t *testing.T) {
	path := writeTempFile(t, sampleYAML)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
		watchFile: false,
	}

	notifier := &fakeNotifier{}
	err := fd.load(context.Background(), notifier)
	require.NoError(t, err)

	assert.Len(t, notifier.upserts, 2)

	// Check first backend.
	first := notifier.upserts[0]
	assert.Equal(t, "vllm-0", first.NamespacedName.Name)
	assert.Equal(t, "ns1", first.NamespacedName.Namespace)
	assert.Equal(t, "10.0.0.1", first.Address)
	assert.Equal(t, "8000", first.Port)
	assert.Equal(t, "10.0.0.1:9090", first.MetricsHost)
	assert.Equal(t, map[string]string{"model": "llama3"}, first.Labels)

	// Check second backend — metricsHost defaulted from address:port.
	second := notifier.upserts[1]
	assert.Equal(t, "vllm-1", second.NamespacedName.Name)
	assert.Equal(t, "10.0.0.2:8000", second.MetricsHost)
}

// TestLoad_DefaultNamespace verifies that missing namespace defaults to "default".
func TestLoad_DefaultNamespace(t *testing.T) {
	content := `
backends:
  - name: vllm-0
    address: "10.0.0.1"
    port: "8000"
`
	path := writeTempFile(t, content)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
	}

	notifier := &fakeNotifier{}
	require.NoError(t, fd.load(context.Background(), notifier))
	assert.Equal(t, "default", notifier.upserts[0].NamespacedName.Namespace)
}

// TestLoad_InvalidYAML verifies that malformed YAML returns an error.
func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTempFile(t, "{ invalid yaml {{{{")
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
	}
	err := fd.load(context.Background(), &fakeNotifier{})
	assert.Error(t, err)
}

// TestLoad_FileNotFound verifies that a missing file returns an error.
func TestLoad_FileNotFound(t *testing.T) {
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      "/nonexistent/path/backends.yaml",
	}
	err := fd.load(context.Background(), &fakeNotifier{})
	assert.Error(t, err)
}

// TestStart_NoWatch verifies that Start calls load and MarkSynced then blocks until cancel.
func TestStart_NoWatch(t *testing.T) {
	path := writeTempFile(t, sampleYAML)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
		watchFile: false,
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- fd.Start(ctx, notifier) }()

	// Wait until MarkSynced is called.
	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, time.Second, 10*time.Millisecond, "MarkSynced not called")

	assert.Equal(t, 2, notifier.upsertCount())

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// TestStart_WatchFile verifies that a file change triggers a reload with correct upserts and deletes.
func TestStart_WatchFile(t *testing.T) {
	path := writeTempFile(t, sampleYAML)
	fd := &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		path:      path,
		watchFile: true,
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- fd.Start(ctx, notifier) }()

	// Wait for initial sync (2 backends).
	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, 2*time.Second, 10*time.Millisecond, "initial MarkSynced not called")

	assert.Equal(t, 2, notifier.upsertCount())

	// Replace file with one new backend — vllm-0 and vllm-1 should be deleted, vllm-new upserted.
	newContent := `
backends:
  - name: vllm-new
    namespace: ns1
    address: "10.0.0.99"
    port: "8000"
`
	require.NoError(t, os.WriteFile(path, []byte(newContent), 0o644))

	// Wait for the deletion of the two original backends.
	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return len(notifier.deletes) == 2
	}, 3*time.Second, 50*time.Millisecond, "expected 2 deletions after file reload")

	notifier.mu.Lock()
	deletedNames := make([]string, len(notifier.deletes))
	for i, d := range notifier.deletes {
		deletedNames[i] = d.Name
	}
	notifier.mu.Unlock()
	assert.ElementsMatch(t, []string{"vllm-0", "vllm-1"}, deletedNames)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}
