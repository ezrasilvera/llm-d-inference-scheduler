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

// Package http provides an HTTP-polling BackendDiscovery implementation.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const PluginType = "http-backend-discovery"

const defaultRefreshInterval = 30 * time.Second

// BackendEntry is the JSON representation of a single backend returned by the HTTP endpoint.
type BackendEntry struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
	Address     string            `json:"address"`
	Port        string            `json:"port"`
	MetricsHost string            `json:"metricsHost,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type params struct {
	URL             string        `json:"url"`
	RefreshInterval time.Duration `json:"refreshInterval"`
}

// HTTPDiscovery implements BackendDiscovery by polling an HTTP endpoint.
type HTTPDiscovery struct {
	typedName       fwkplugin.TypedName
	url             string
	refreshInterval time.Duration
	client          *http.Client
}

var _ discovery.BackendDiscovery = (*HTTPDiscovery)(nil)
var _ fwkplugin.Plugin = (*HTTPDiscovery)(nil)

// Factory is the plugin factory for http-backend-discovery.
func Factory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &params{RefreshInterval: defaultRefreshInterval}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("http-backend-discovery: failed to parse parameters: %w", err)
		}
	}
	if p.URL == "" {
		return nil, fmt.Errorf("http-backend-discovery: 'url' parameter is required")
	}
	if p.RefreshInterval <= 0 {
		p.RefreshInterval = defaultRefreshInterval
	}
	if name == "" {
		name = PluginType
	}
	return &HTTPDiscovery{
		typedName:       fwkplugin.TypedName{Type: PluginType, Name: name},
		url:             p.URL,
		refreshInterval: p.RefreshInterval,
		client:          &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (h *HTTPDiscovery) TypedName() fwkplugin.TypedName { return h.typedName }

// Start polls the HTTP endpoint on each refresh interval and diffs against the current set.
func (h *HTTPDiscovery) Start(ctx context.Context, notifier discovery.Notifier) error {
	logger := log.FromContext(ctx).WithValues("plugin", PluginType, "url", h.url)

	current := make(map[types.NamespacedName]struct{})

	poll := func() {
		entries, err := h.fetch(ctx)
		if err != nil {
			logger.Error(err, "failed to fetch backends")
			return
		}

		incoming := make(map[types.NamespacedName]struct{}, len(entries))
		for _, b := range entries {
			ns := b.Namespace
			if ns == "" {
				ns = "default"
			}
			id := types.NamespacedName{Name: b.Name, Namespace: ns}
			incoming[id] = struct{}{}
			meta := &fwkdl.EndpointMetadata{
				NamespacedName: id,
				PodName:        b.Name,
				Address:        b.Address,
				Port:           b.Port,
				MetricsHost:    b.MetricsHost,
				Labels:         b.Labels,
			}
			if meta.MetricsHost == "" {
				meta.MetricsHost = fmt.Sprintf("%s:%s", b.Address, b.Port)
			}
			notifier.Upsert(meta)
		}

		for id := range current {
			if _, ok := incoming[id]; !ok {
				notifier.Delete(id)
			}
		}
		current = incoming
	}

	poll()
	notifier.MarkSynced()

	ticker := time.NewTicker(h.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			poll()
		}
	}
}

func (h *HTTPDiscovery) fetch(ctx context.Context) ([]BackendEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, h.url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var entries []BackendEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}
	return entries, nil
}
