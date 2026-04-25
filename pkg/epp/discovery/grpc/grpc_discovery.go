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

// Package grpc provides a gRPC-streaming BackendDiscovery implementation.
// It connects to a server-streaming RPC endpoint that pushes BackendEvent messages.
package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const PluginType = "grpc-backend-discovery"

const (
	defaultReconnectDelay    = 2 * time.Second
	defaultMaxReconnectDelay = 30 * time.Second
)

// EventType distinguishes add/update from delete events in the stream.
type EventType string

const (
	EventTypeUpsert EventType = "UPSERT"
	EventTypeDelete EventType = "DELETE"
)

// BackendEvent is the wire format for a single event pushed by the gRPC server.
type BackendEvent struct {
	Type      EventType         `json:"type"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Address   string            `json:"address,omitempty"`
	Port      string            `json:"port,omitempty"`
	MetricsHost string          `json:"metricsHost,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// BackendEventStream is the minimal gRPC client stream interface expected from the server.
// The server must implement a streaming RPC that sends BackendEvent messages encoded as JSON.
type BackendEventStream interface {
	Recv() (*BackendEvent, error)
	grpc.ClientStream
}

type params struct {
	Address string `json:"address"`
	Insecure bool   `json:"insecure"`
}

// GRPCDiscovery implements BackendDiscovery by subscribing to a gRPC event stream.
type GRPCDiscovery struct {
	typedName fwkplugin.TypedName
	address   string
	insecure  bool
}

var _ discovery.BackendDiscovery = (*GRPCDiscovery)(nil)
var _ fwkplugin.Plugin = (*GRPCDiscovery)(nil)

// Factory is the plugin factory for grpc-backend-discovery.
func Factory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &params{Insecure: true}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("grpc-backend-discovery: failed to parse parameters: %w", err)
		}
	}
	if p.Address == "" {
		return nil, fmt.Errorf("grpc-backend-discovery: 'address' parameter is required")
	}
	if name == "" {
		name = PluginType
	}
	return &GRPCDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		address:   p.Address,
		insecure:  p.Insecure,
	}, nil
}

func (g *GRPCDiscovery) TypedName() fwkplugin.TypedName { return g.typedName }

// Start connects to the gRPC server and processes events with exponential backoff on reconnect.
func (g *GRPCDiscovery) Start(ctx context.Context, notifier discovery.Notifier) error {
	logger := log.FromContext(ctx).WithValues("plugin", PluginType, "address", g.address)

	synced := false
	delay := defaultReconnectDelay

	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := g.stream(ctx, notifier, &synced, logger); err != nil {
			logger.Error(err, "stream error, reconnecting", "delay", delay)
		}

		if ctx.Err() != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		delay *= 2
		if delay > defaultMaxReconnectDelay {
			delay = defaultMaxReconnectDelay
		}
	}
}

func (g *GRPCDiscovery) stream(ctx context.Context, notifier discovery.Notifier, synced *bool, logger interface{ Error(error, string, ...any) }) error {
	opts := []grpc.DialOption{}
	if g.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(g.address, opts...)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", g.address, err)
	}
	defer conn.Close()

	// The BackendDiscoveryServiceClient and WatchBackends RPC are expected to be
	// provided by the gRPC server. Use the generic streaming call.
	desc := &grpc.StreamDesc{ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, "/discovery.BackendDiscoveryService/WatchBackends")
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}

	// Send an empty request to initiate the stream.
	if err := stream.SendMsg(&struct{}{}); err != nil {
		return fmt.Errorf("sending init message: %w", err)
	}

	for {
		var event BackendEvent
		if err := stream.RecvMsg(&event); err != nil {
			return fmt.Errorf("receiving event: %w", err)
		}

		ns := event.Namespace
		if ns == "" {
			ns = "default"
		}
		id := types.NamespacedName{Name: event.Name, Namespace: ns}

		switch event.Type {
		case EventTypeDelete:
			notifier.Delete(id)
		default:
			meta := &fwkdl.EndpointMetadata{
				NamespacedName: id,
				PodName:        event.Name,
				Address:        event.Address,
				Port:           event.Port,
				MetricsHost:    event.MetricsHost,
				Labels:         event.Labels,
			}
			if meta.MetricsHost == "" && meta.Address != "" && meta.Port != "" {
				meta.MetricsHost = fmt.Sprintf("%s:%s", meta.Address, meta.Port)
			}
			notifier.Upsert(meta)
		}

		if !*synced {
			notifier.MarkSynced()
			*synced = true
		}
	}
}
