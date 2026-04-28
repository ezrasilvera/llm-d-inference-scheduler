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

// Package file provides a file-based DiscoveryPlugin implementation that reads
// a YAML (or JSON) file listing inference endpoints.
package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const PluginType = "file-discovery"

// EndpointEntry is the YAML/JSON representation of a single endpoint in the endpoints file.
type EndpointEntry struct {
	Name        string            `json:"name" yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Address     string            `json:"address" yaml:"address"`
	Port        string            `json:"port" yaml:"port"`
	MetricsHost string            `json:"metricsHost,omitempty" yaml:"metricsHost,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
}

// EndpointsFile is the top-level structure of the endpoints YAML/JSON file.
type EndpointsFile struct {
	Endpoints []EndpointEntry `json:"endpoints" yaml:"endpoints"`
}

type params struct {
	Path      string `json:"path"`
	WatchFile bool   `json:"watchFile"`
}

// FileDiscovery implements DiscoveryPlugin by reading a static endpoints file.
type FileDiscovery struct {
	typedName fwkplugin.TypedName
	path      string
	watchFile bool
	current   map[types.NamespacedName]struct{}
}

var _ discovery.DiscoveryPlugin = (*FileDiscovery)(nil)
var _ fwkplugin.Plugin = (*FileDiscovery)(nil)

// Factory is the plugin factory for file-discovery.
func Factory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &params{WatchFile: false}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("file-discovery: failed to parse parameters: %w", err)
		}
	}
	if p.Path == "" {
		return nil, fmt.Errorf("file-discovery: 'path' parameter is required")
	}
	if name == "" {
		name = PluginType
	}
	return &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		path:      p.Path,
		watchFile: p.WatchFile,
		current:   make(map[types.NamespacedName]struct{}),
	}, nil
}

func (f *FileDiscovery) TypedName() fwkplugin.TypedName { return f.typedName }

// Start loads the endpoints file, notifies the datastore, then optionally watches for changes.
// Blocks until ctx is cancelled or a fatal error occurs.
func (f *FileDiscovery) Start(ctx context.Context, notifier discovery.Notifier) error {
	logger := log.FromContext(ctx).WithValues("plugin", PluginType, "path", f.path)

	if err := f.load(ctx, notifier); err != nil {
		return fmt.Errorf("file-discovery: initial load failed: %w", err)
	}

	if !f.watchFile {
		<-ctx.Done()
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("file-discovery: failed to create watcher: %w", err)
	}
	defer watcher.Close()

	if err := watcher.Add(f.path); err != nil {
		return fmt.Errorf("file-discovery: failed to watch %s: %w", f.path, err)
	}

	logger.Info("watching endpoints file for changes")
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				logger.Info("endpoints file changed, reloading")
				if err := f.load(ctx, notifier); err != nil {
					logger.Error(err, "failed to reload endpoints file")
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			logger.Error(err, "watcher error")
		}
	}
}

// load reads the file, calls notifier.Upsert for all endpoints in one batch, and
// calls notifier.Delete for any endpoint that was present in the previous load but is absent now.
func (f *FileDiscovery) load(_ context.Context, notifier discovery.Notifier) error {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", f.path, err)
	}

	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return fmt.Errorf("parsing %s: %w", f.path, err)
	}

	var ef EndpointsFile
	if err := json.Unmarshal(jsonData, &ef); err != nil {
		return fmt.Errorf("unmarshalling %s: %w", f.path, err)
	}

	incoming := make(map[types.NamespacedName]struct{}, len(ef.Endpoints))
	batch := make([]*fwkdl.EndpointMetadata, 0, len(ef.Endpoints))

	for _, e := range ef.Endpoints {
		ns := e.Namespace
		if ns == "" {
			ns = "default"
		}
		meta := &fwkdl.EndpointMetadata{
			NamespacedName: types.NamespacedName{Name: e.Name, Namespace: ns},
			PodName:        e.Name,
			Address:        e.Address,
			Port:           e.Port,
			MetricsHost:    e.MetricsHost,
			Labels:         e.Labels,
		}
		if meta.MetricsHost == "" {
			meta.MetricsHost = fmt.Sprintf("%s:%s", e.Address, e.Port)
		}
		incoming[meta.NamespacedName] = struct{}{}
		batch = append(batch, meta)
	}

	// Upsert all current endpoints in a single ordered call.
	if len(batch) > 0 {
		notifier.Upsert(batch)
	}

	// Delete endpoints absent from the new file version.
	for id := range f.current {
		if _, ok := incoming[id]; !ok {
			notifier.Delete(id)
		}
	}
	f.current = incoming

	return nil
}
