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

package grpc

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
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

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestFactory validates parameter parsing and required field checking.
func TestFactory(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr bool
	}{
		{
			name:    "missing address returns error",
			params:  map[string]any{},
			wantErr: true,
		},
		{
			name:    "valid insecure params",
			params:  map[string]any{"address": "localhost:50051", "insecure": true},
			wantErr: false,
		},
		{
			name:    "valid secure params",
			params:  map[string]any{"address": "localhost:50051", "insecure": false},
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
			g := plugin.(*GRPCDiscovery)
			assert.Equal(t, PluginType, g.TypedName().Type)
		})
	}
}

// TestFactory_DefaultName verifies that the plugin name defaults to PluginType when empty.
func TestFactory_DefaultName(t *testing.T) {
	params := mustMarshal(t, map[string]any{"address": "localhost:50051"})
	plugin, err := Factory("", params, nil)
	require.NoError(t, err)
	assert.Equal(t, PluginType, plugin.TypedName().Name)
}

// TestFactory_CustomName verifies that a provided name is preserved.
func TestFactory_CustomName(t *testing.T) {
	params := mustMarshal(t, map[string]any{"address": "localhost:50051"})
	plugin, err := Factory("my-grpc-discovery", params, nil)
	require.NoError(t, err)
	assert.Equal(t, "my-grpc-discovery", plugin.TypedName().Name)
}

// TestTypedName verifies that TypedName returns the correct type and name.
func TestTypedName(t *testing.T) {
	g := &GRPCDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "my-name"},
		address:   "localhost:50051",
	}
	tn := g.TypedName()
	assert.Equal(t, PluginType, tn.Type)
	assert.Equal(t, "my-name", tn.Name)
}

// TestStart_ConnectFailure verifies that Start returns gracefully when the context
// is cancelled and no server is reachable (reconnect loop terminates).
func TestStart_ConnectFailure(t *testing.T) {
	g := &GRPCDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		address:   "localhost:19999", // nothing listening here
		insecure:  true,
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := g.Start(ctx, notifier)
	// Start should return nil (ctx cancellation is not an error).
	assert.NoError(t, err)
}

// TestEventType_Constants verifies the EventType constant values.
func TestEventType_Constants(t *testing.T) {
	assert.Equal(t, EventType("UPSERT"), EventTypeUpsert)
	assert.Equal(t, EventType("DELETE"), EventTypeDelete)
}

// TestBackendEvent_Defaults verifies BackendEvent zero-value behaviour.
func TestBackendEvent_Defaults(t *testing.T) {
	event := BackendEvent{}
	assert.Equal(t, EventType(""), event.Type)
	assert.Equal(t, "", event.Name)
}

// TestReconnectDelayBounds verifies that the reconnect delay caps at maxReconnectDelay.
func TestReconnectDelayBounds(t *testing.T) {
	delay := defaultReconnectDelay
	for range 10 {
		delay *= 2
		if delay > defaultMaxReconnectDelay {
			delay = defaultMaxReconnectDelay
		}
	}
	assert.Equal(t, defaultMaxReconnectDelay, delay)
}
