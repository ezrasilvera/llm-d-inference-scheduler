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

// Package file provides a file-based BackendDiscovery implementation that reads
// a YAML/JSON file listing inference backends.
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

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const PluginType = "file-backend-discovery"

// BackendEntry is the YAML/JSON representation of a single backend in the backends file.
type BackendEntry struct {
	Name        string            `json:"name" yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Address     string            `json:"address" yaml:"address"`
	Port        string            `json:"port" yaml:"port"`
	MetricsHost string            `json:"metricsHost,omitempty" yaml:"metricsHost,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
}

// BackendsFile is the top-level structure of the backends YAML/JSON file.
type BackendsFile struct {
	Backends []BackendEntry `json:"backends" yaml:"backends"`
}

type params struct {
	Path      string `json:"path"`
	WatchFile bool   `json:"watchFile"`
}

// FileDiscovery implements BackendDiscovery by reading a static backends file.
type FileDiscovery struct {
	typedName fwkplugin.TypedName
	path      string
	watchFile bool
	current   map[types.NamespacedName]struct{} // tracks the last known set for deletion diffing
}

var _ discovery.BackendDiscovery = (*FileDiscovery)(nil)
var _ fwkplugin.Plugin = (*FileDiscovery)(nil)

// Factory is the plugin factory for file-backend-discovery.
func Factory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &params{WatchFile: false}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("file-backend-discovery: failed to parse parameters: %w", err)
		}
	}
	if p.Path == "" {
		return nil, fmt.Errorf("file-backend-discovery: 'path' parameter is required")
	}
	if name == "" {
		name = PluginType
	}
	return &FileDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		path:      p.Path,
		watchFile: p.WatchFile,
	}, nil
}

func (f *FileDiscovery) TypedName() fwkplugin.TypedName { return f.typedName }

// Start loads the backends file, notifies the datastore, then optionally watches for changes.
func (f *FileDiscovery) Start(ctx context.Context, notifier discovery.Notifier) error {
	logger := log.FromContext(ctx).WithValues("plugin", PluginType, "path", f.path)

	if err := f.load(ctx, notifier); err != nil {
		return fmt.Errorf("file-backend-discovery: initial load failed: %w", err)
	}
	notifier.MarkSynced()

	if !f.watchFile {
		<-ctx.Done()
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("file-backend-discovery: failed to create watcher: %w", err)
	}
	defer watcher.Close()

	if err := watcher.Add(f.path); err != nil {
		return fmt.Errorf("file-backend-discovery: failed to watch %s: %w", f.path, err)
	}

	logger.Info("watching backends file for changes")
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				logger.Info("backends file changed, reloading")
				if err := f.load(ctx, notifier); err != nil {
					logger.Error(err, "failed to reload backends file")
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

func (f *FileDiscovery) load(ctx context.Context, notifier discovery.Notifier) error {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", f.path, err)
	}

	// Accept both YAML and JSON.
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return fmt.Errorf("parsing %s: %w", f.path, err)
	}

	var bf BackendsFile
	if err := json.Unmarshal(jsonData, &bf); err != nil {
		return fmt.Errorf("unmarshalling %s: %w", f.path, err)
	}

	incoming := make(map[types.NamespacedName]struct{}, len(bf.Backends))
	for _, b := range bf.Backends {
		ns := b.Namespace
		if ns == "" {
			ns = "default"
		}
		meta := &fwkdl.EndpointMetadata{
			NamespacedName: types.NamespacedName{Name: b.Name, Namespace: ns},
			PodName:        b.Name,
			Address:        b.Address,
			Port:           b.Port,
			MetricsHost:    b.MetricsHost,
			Labels:         b.Labels,
		}
		if meta.MetricsHost == "" {
			meta.MetricsHost = fmt.Sprintf("%s:%s", b.Address, b.Port)
		}
		incoming[meta.NamespacedName] = struct{}{}
		notifier.Upsert(meta)
	}

	// Delete backends that were present in the previous load but are absent now.
	for id := range f.current {
		if _, ok := incoming[id]; !ok {
			notifier.Delete(id)
		}
	}
	f.current = incoming
	_ = ctx

	return nil
}
