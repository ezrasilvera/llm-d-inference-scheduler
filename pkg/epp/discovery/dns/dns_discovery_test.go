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

package dns

import (
	"context"
	"encoding/json"
	"net"
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

// fakeResolver is a test double for the dnsResolver interface.
type fakeResolver struct {
	mu      sync.Mutex
	srvResp []*net.SRV
	srvErr  error
	hostMap map[string][]string // hostname → IPs
	hostErr error
}

func (r *fakeResolver) LookupSRV(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return "", r.srvResp, r.srvErr
}

func (r *fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hostErr != nil {
		return nil, r.hostErr
	}
	return r.hostMap[host], nil
}

func (r *fakeResolver) setSRV(srvs []*net.SRV) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.srvResp = srvs
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestFactory validates parameter parsing for all modes.
func TestFactory(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		wantErr bool
	}{
		{
			name:    "srv mode missing service",
			params:  map[string]any{"dnsMode": "srv", "domain": "svc.cluster.local"},
			wantErr: true,
		},
		{
			name:    "srv mode missing domain",
			params:  map[string]any{"dnsMode": "srv", "service": "vllm"},
			wantErr: true,
		},
		{
			name:   "srv mode valid",
			params: map[string]any{"dnsMode": "srv", "service": "vllm", "proto": "tcp", "domain": "svc.cluster.local"},
		},
		{
			name:    "a mode missing host",
			params:  map[string]any{"dnsMode": "a", "port": "8000"},
			wantErr: true,
		},
		{
			name:    "a mode missing port",
			params:  map[string]any{"dnsMode": "a", "host": "vllm.svc.cluster.local"},
			wantErr: true,
		},
		{
			name:   "a mode valid",
			params: map[string]any{"dnsMode": "a", "host": "vllm.svc.cluster.local", "port": "8000"},
		},
		{
			name:    "unknown mode",
			params:  map[string]any{"dnsMode": "consul"},
			wantErr: true,
		},
		{
			name:   "srv mode default (no dnsMode field)",
			params: map[string]any{"service": "vllm", "domain": "svc.cluster.local"},
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

// TestFactory_DefaultName verifies the plugin name defaults to PluginType.
func TestFactory_DefaultName(t *testing.T) {
	params := mustMarshal(t, map[string]any{"service": "vllm", "domain": "svc.local"})
	plugin, err := Factory("", params, nil)
	require.NoError(t, err)
	assert.Equal(t, PluginType, plugin.TypedName().Name)
}

// TestResolveA verifies that A-record resolution produces correct EndpointMetadata.
func TestResolveA(t *testing.T) {
	resolver := &fakeResolver{
		hostMap: map[string][]string{
			"vllm.svc.local": {"10.0.0.1", "10.0.0.2"},
		},
	}

	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:   ModeA,
			Host:      "vllm.svc.local",
			Port:      "8000",
			Namespace: "default",
		},
		resolver: resolver,
	}

	results, err := d.resolveA(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, "10.0.0.1", results[0].Address)
	assert.Equal(t, "8000", results[0].Port)
	assert.Equal(t, "default", results[0].NamespacedName.Namespace)
	assert.Equal(t, "10.0.0.1:8000", results[0].MetricsHost)

	assert.Equal(t, "10.0.0.2", results[1].Address)
}

// TestResolveA_Empty verifies empty result when host resolves to nothing.
func TestResolveA_Empty(t *testing.T) {
	resolver := &fakeResolver{
		hostMap: map[string][]string{},
	}
	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p:         params{DNSMode: ModeA, Host: "gone.svc.local", Port: "8000", Namespace: "default"},
		resolver:  resolver,
	}
	results, err := d.resolveA(context.Background())
	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestResolveSRV verifies that SRV-record resolution produces correct EndpointMetadata.
func TestResolveSRV(t *testing.T) {
	resolver := &fakeResolver{
		srvResp: []*net.SRV{
			{Target: "pod-0.svc.local.", Port: 8000},
			{Target: "pod-1.svc.local.", Port: 8001},
		},
		hostMap: map[string][]string{
			"pod-0.svc.local.": {"10.1.1.1"},
			"pod-1.svc.local.": {"10.1.1.2"},
		},
	}

	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:   ModeSRV,
			Service:   "vllm",
			Proto:     "tcp",
			Domain:    "svc.local",
			Namespace: "ns1",
			Labels:    map[string]string{"model": "llama3"},
		},
		resolver: resolver,
	}

	results, err := d.resolveSRV(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, "10.1.1.1", results[0].Address)
	assert.Equal(t, "8000", results[0].Port)
	assert.Equal(t, "ns1", results[0].NamespacedName.Namespace)
	assert.Equal(t, map[string]string{"model": "llama3"}, results[0].Labels)

	assert.Equal(t, "10.1.1.2", results[1].Address)
	assert.Equal(t, "8001", results[1].Port)
}

// TestResolveSRV_SkipsUnresolvableTargets verifies that SRV targets that fail host lookup are skipped.
func TestResolveSRV_SkipsUnresolvableTargets(t *testing.T) {
	resolver := &fakeResolver{
		srvResp: []*net.SRV{
			{Target: "good.svc.local.", Port: 8000},
			{Target: "bad.svc.local.", Port: 8001},
		},
		hostMap: map[string][]string{
			"good.svc.local.": {"10.1.1.1"},
			// "bad.svc.local." has no entry → empty result, skipped
		},
	}

	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:   ModeSRV,
			Service:   "vllm",
			Proto:     "tcp",
			Domain:    "svc.local",
			Namespace: "default",
		},
		resolver: resolver,
	}

	results, err := d.resolveSRV(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "10.1.1.1", results[0].Address)
}

// TestStart_AMode_DiffOnPoll verifies that backends disappearing in a subsequent poll are deleted.
func TestStart_AMode_DiffOnPoll(t *testing.T) {
	resolver := &fakeResolver{
		hostMap: map[string][]string{
			"vllm.svc.local": {"10.0.0.1", "10.0.0.2"},
		},
	}

	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:         ModeA,
			Host:            "vllm.svc.local",
			Port:            "8000",
			Namespace:       "default",
			RefreshInterval: 20 * time.Millisecond,
		},
		resolver: resolver,
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.Start(ctx, notifier) //nolint:errcheck

	// Wait for initial sync with two backends.
	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, time.Second, 10*time.Millisecond)

	// Now one backend disappears.
	resolver.mu.Lock()
	resolver.hostMap["vllm.svc.local"] = []string{"10.0.0.1"}
	resolver.mu.Unlock()

	// Wait for a deletion.
	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return len(notifier.deletes) > 0
	}, 2*time.Second, 20*time.Millisecond, "deletion not triggered after backend removal")

	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	assert.Len(t, notifier.deletes, 1)
}

// TestStart_MarkSynced verifies that MarkSynced is called even when the initial result is empty.
func TestStart_MarkSynced(t *testing.T) {
	resolver := &fakeResolver{hostMap: map[string][]string{}}
	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:         ModeA,
			Host:            "empty.svc.local",
			Port:            "8000",
			Namespace:       "default",
			RefreshInterval: time.Hour,
		},
		resolver: resolver,
	}

	notifier := &fakeNotifier{}
	ctx, cancel := context.WithCancel(context.Background())

	go d.Start(ctx, notifier) //nolint:errcheck

	require.Eventually(t, func() bool {
		notifier.mu.Lock()
		defer notifier.mu.Unlock()
		return notifier.synced
	}, time.Second, 10*time.Millisecond, "MarkSynced not called")

	cancel()
}

// TestLabelsAttached verifies that configured labels are propagated to all discovered backends.
func TestLabelsAttached(t *testing.T) {
	resolver := &fakeResolver{
		hostMap: map[string][]string{"vllm.svc.local": {"10.0.0.1"}},
	}
	d := &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: "test"},
		p: params{
			DNSMode:         ModeA,
			Host:            "vllm.svc.local",
			Port:            "8000",
			Namespace:       "default",
			Labels:          map[string]string{"env": "prod", "model": "qwen"},
			RefreshInterval: time.Hour,
		},
		resolver: resolver,
	}

	results, err := d.resolveA(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, map[string]string{"env": "prod", "model": "qwen"}, results[0].Labels)
}
