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

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
)

// fakeNotifier records all calls.
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

func (n *fakeNotifier) deleteCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.deletes)
}

func (n *fakeNotifier) upsertCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.upserts)
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestFactory validates parameter parsing and error cases.
func TestFactory(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr bool
	}{
		{
			name:    "missing url returns error",
			params:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid minimal params",
			params:  map[string]any{"url": "http://localhost:8080/backends"},
			wantErr: false,
		},
		{
			name:    "custom refresh interval",
			params:  map[string]any{"url": "http://localhost/backends", "refreshInterval": int64(10 * time.Second)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin, err := Factory("test", mustMarshal(t, tt.params), nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, PluginType, plugin.TypedName().Type)
		})
	}
}

// TestStart_InitialPoll verifies that the first poll calls Upsert for all returned backends
// and then calls MarkSynced.
func TestStart_InitialPoll(t *testing.T) {
	backends := []BackendEntry{
		{Name: "b0", Namespace: "ns", Address: "1.2.3.4", Port: "8000", MetricsHost: "1.2.3.4:9090"},
		{Name: "b1", Namespace: "ns", Address: "1.2.3.5", Port: "8000"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(backends)
	}))
	defer srv.Close()

	hd := &HTTPDiscovery{
		url:             srv.URL,
		refreshInterval: time.Hour, // only one poll
		client:          srv.Client(),
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- hd.Start(ctx, notifier) }()

	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, time.Second, 10*time.Millisecond, "MarkSynced not called")

	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	assert.Len(t, notifier.upserts, 2)
	// Explicit MetricsHost preserved.
	assert.Equal(t, "1.2.3.4:9090", notifier.upserts[0].MetricsHost)
	// Derived MetricsHost when not set.
	assert.Equal(t, "1.2.3.5:8000", notifier.upserts[1].MetricsHost)

	cancel()
	<-done
}

// TestStart_DiffDetectsRemovals verifies that backends absent from the second poll are deleted.
func TestStart_DiffDetectsRemovals(t *testing.T) {
	var callCount atomic.Int32

	first := []BackendEntry{
		{Name: "keep", Namespace: "ns", Address: "1.1.1.1", Port: "8000"},
		{Name: "gone", Namespace: "ns", Address: "2.2.2.2", Port: "8000"},
	}
	second := []BackendEntry{
		{Name: "keep", Namespace: "ns", Address: "1.1.1.1", Port: "8000"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if callCount.Add(1) == 1 {
			json.NewEncoder(w).Encode(first)
		} else {
			json.NewEncoder(w).Encode(second)
		}
	}))
	defer srv.Close()

	hd := &HTTPDiscovery{
		url:             srv.URL,
		refreshInterval: 20 * time.Millisecond,
		client:          srv.Client(),
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hd.Start(ctx, notifier) //nolint:errcheck

	// Wait for the deletion from the second poll.
	require.Eventually(t, func() bool {
		return notifier.deleteCount() > 0
	}, 2*time.Second, 20*time.Millisecond, "no deletion triggered")

	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	require.Len(t, notifier.deletes, 1)
	assert.Equal(t, types.NamespacedName{Name: "gone", Namespace: "ns"}, notifier.deletes[0])
}

// TestStart_DiffDetectsAdditions verifies that new backends on the second poll are upserted.
func TestStart_DiffDetectsAdditions(t *testing.T) {
	var callCount atomic.Int32

	first := []BackendEntry{{Name: "existing", Namespace: "ns", Address: "1.1.1.1", Port: "8000"}}
	second := []BackendEntry{
		{Name: "existing", Namespace: "ns", Address: "1.1.1.1", Port: "8000"},
		{Name: "new", Namespace: "ns", Address: "2.2.2.2", Port: "8000"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if callCount.Add(1) == 1 {
			json.NewEncoder(w).Encode(first)
		} else {
			json.NewEncoder(w).Encode(second)
		}
	}))
	defer srv.Close()

	hd := &HTTPDiscovery{
		url:             srv.URL,
		refreshInterval: 20 * time.Millisecond,
		client:          srv.Client(),
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hd.Start(ctx, notifier) //nolint:errcheck

	// Wait until the new backend shows up.
	require.Eventually(t, func() bool {
		return notifier.upsertCount() >= 3 // 1 initial + 1 existing re-upsert + 1 new
	}, 2*time.Second, 20*time.Millisecond, "new backend upsert not triggered")
}

// TestStart_HTTPError verifies that a server error does not crash Start.
func TestStart_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	hd := &HTTPDiscovery{
		url:             srv.URL,
		refreshInterval: 20 * time.Millisecond,
		client:          srv.Client(),
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start should return without crashing once ctx expires.
	err := hd.Start(ctx, notifier)
	assert.NoError(t, err)
	// MarkSynced is called even when the first poll errors (empty result = synced).
	assert.True(t, notifier.synced)
}

// TestStart_DefaultNamespace verifies that a missing namespace defaults to "default".
func TestStart_DefaultNamespace(t *testing.T) {
	backends := []BackendEntry{{Name: "b0", Address: "1.2.3.4", Port: "8000"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(backends)
	}))
	defer srv.Close()

	hd := &HTTPDiscovery{
		url:             srv.URL,
		refreshInterval: time.Hour,
		client:          srv.Client(),
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())

	go hd.Start(ctx, notifier) //nolint:errcheck

	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, time.Second, 10*time.Millisecond)

	cancel()
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	assert.Equal(t, "default", notifier.upserts[0].NamespacedName.Namespace)
}
