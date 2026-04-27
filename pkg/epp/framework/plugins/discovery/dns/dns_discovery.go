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

// Package dns provides a DNS-based BackendDiscovery implementation.
// It supports SRV record queries and A/AAAA record queries.
package dns

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const PluginType = "dns-backend-discovery"

const defaultRefreshInterval = 30 * time.Second

// DNSMode selects the DNS query strategy.
type DNSMode string

const (
	// ModeSRV queries _<service>._<proto>.<domain> SRV records.
	ModeSRV DNSMode = "srv"
	// ModeA queries A/AAAA records for a hostname directly.
	ModeA DNSMode = "a"
)

type params struct {
	// DNSMode selects SRV or A record mode. Default: "srv".
	DNSMode DNSMode `json:"dnsMode"`

	// SRV mode fields.
	Service string `json:"service"`
	Proto   string `json:"proto"`
	Domain  string `json:"domain"`

	// A mode fields.
	Host string `json:"host"`
	Port string `json:"port"`

	// Common fields.
	RefreshInterval time.Duration     `json:"refreshInterval"`
	Namespace       string            `json:"namespace"`
	Labels          map[string]string `json:"labels"`
}

// dnsResolver is the minimal interface used for DNS lookups, allowing injection in tests.
type dnsResolver interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// DNSDiscovery implements BackendDiscovery by polling DNS records.
type DNSDiscovery struct {
	typedName fwkplugin.TypedName
	p         params
	resolver  dnsResolver // defaults to net.DefaultResolver; injectable for tests
}

var _ discovery.BackendDiscovery = (*DNSDiscovery)(nil)
var _ fwkplugin.Plugin = (*DNSDiscovery)(nil)

// Factory is the plugin factory for dns-backend-discovery.
func Factory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := params{
		DNSMode:         ModeSRV,
		Proto:           "tcp",
		RefreshInterval: defaultRefreshInterval,
		Namespace:       "default",
	}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, &p); err != nil {
			return nil, fmt.Errorf("dns-backend-discovery: failed to parse parameters: %w", err)
		}
	}
	if p.RefreshInterval <= 0 {
		p.RefreshInterval = defaultRefreshInterval
	}
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	switch p.DNSMode {
	case ModeSRV:
		if p.Service == "" || p.Domain == "" {
			return nil, fmt.Errorf("dns-backend-discovery (srv): 'service' and 'domain' are required")
		}
	case ModeA:
		if p.Host == "" || p.Port == "" {
			return nil, fmt.Errorf("dns-backend-discovery (a): 'host' and 'port' are required")
		}
	default:
		return nil, fmt.Errorf("dns-backend-discovery: unknown dnsMode %q (use 'srv' or 'a')", p.DNSMode)
	}

	if name == "" {
		name = PluginType
	}
	return &DNSDiscovery{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		p:         p,
		resolver:  net.DefaultResolver,
	}, nil
}

func (d *DNSDiscovery) TypedName() fwkplugin.TypedName { return d.typedName }

// Start polls DNS on the configured interval, diffing against the current backend set.
func (d *DNSDiscovery) Start(ctx context.Context, notifier discovery.Notifier) error {
	logger := log.FromContext(ctx).WithValues("plugin", PluginType, "mode", d.p.DNSMode)

	current := make(map[types.NamespacedName]struct{})

	poll := func() {
		entries, err := d.resolve(ctx)
		if err != nil {
			logger.Error(err, "DNS resolution failed")
			return
		}

		incoming := make(map[types.NamespacedName]struct{}, len(entries))
		for _, meta := range entries {
			incoming[meta.NamespacedName] = struct{}{}
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

	ticker := time.NewTicker(d.p.RefreshInterval)
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

// resolve performs the DNS query and returns EndpointMetadata for each discovered address.
func (d *DNSDiscovery) resolve(ctx context.Context) ([]*fwkdl.EndpointMetadata, error) {
	switch d.p.DNSMode {
	case ModeSRV:
		return d.resolveSRV(ctx)
	case ModeA:
		return d.resolveA(ctx)
	default:
		return nil, fmt.Errorf("unknown dns mode %q", d.p.DNSMode)
	}
}

func (d *DNSDiscovery) resolveSRV(ctx context.Context) ([]*fwkdl.EndpointMetadata, error) {
	r := d.resolver
	_, addrs, err := r.LookupSRV(ctx, d.p.Service, d.p.Proto, d.p.Domain)
	if err != nil {
		return nil, fmt.Errorf("SRV lookup _%.s._%s.%s: %w", d.p.Service, d.p.Proto, d.p.Domain, err)
	}

	var results []*fwkdl.EndpointMetadata
	for _, srv := range addrs {
		// Resolve target hostname to IP.
		ips, err := r.LookupHost(ctx, srv.Target)
		if err != nil || len(ips) == 0 {
			continue
		}
		ip := ips[0]
		port := fmt.Sprintf("%d", srv.Port)
		name := fmt.Sprintf("%s-%d", srv.Target, srv.Port)
		metricsHost := net.JoinHostPort(ip, port)
		meta := &fwkdl.EndpointMetadata{
			NamespacedName: types.NamespacedName{Name: name, Namespace: d.p.Namespace},
			PodName:        name,
			Address:        ip,
			Port:           port,
			MetricsHost:    metricsHost,
			Labels:         d.p.Labels,
		}
		results = append(results, meta)
	}
	return results, nil
}

func (d *DNSDiscovery) resolveA(ctx context.Context) ([]*fwkdl.EndpointMetadata, error) {
	r := d.resolver
	ips, err := r.LookupHost(ctx, d.p.Host)
	if err != nil {
		return nil, fmt.Errorf("A lookup %s: %w", d.p.Host, err)
	}

	var results []*fwkdl.EndpointMetadata
	for i, ip := range ips {
		name := fmt.Sprintf("%s-%d", d.p.Host, i)
		metricsHost := net.JoinHostPort(ip, d.p.Port)
		meta := &fwkdl.EndpointMetadata{
			NamespacedName: types.NamespacedName{Name: name, Namespace: d.p.Namespace},
			PodName:        name,
			Address:        ip,
			Port:           d.p.Port,
			MetricsHost:    metricsHost,
			Labels:         d.p.Labels,
		}
		results = append(results, meta)
	}
	return results, nil
}
