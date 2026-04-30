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

package runner

import (
	"context"
	"encoding/json"

	backendmetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/backend/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	k8sdiscovery "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/discovery/k8s"
	runserver "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/server"
)

// NewTestRunnerSetup creates a datastore for integration tests using the K8s
// InferencePool discovery plugin. The returned datastore is wired up with the
// runner's plugin configuration but the manager is owned by the discovery
// plugin -- call disc.Start(ctx, notifier) to begin reconciliation.
//
// When mockDataSource is non-nil, its plugin type is registered as a factory
// so the YAML config can reference it by type name.
func NewTestRunnerSetup(ctx context.Context, opts *runserver.Options, pmc backendmetrics.PodMetricsClient, mockDataSource fwkdl.DataSource) (datastore.Datastore, *k8sdiscovery.InferencePoolDiscoveryPlugin, error) {
	runner := NewRunner()

	if mockDataSource != nil {
		mockType := mockDataSource.TypedName().Type
		fwkplugin.Register(mockType, func(name string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
			return mockDataSource, nil
		})
		defer delete(fwkplugin.Registry, mockType)
	}

	rawConfig, err := runner.parseConfigurationPhaseOne(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	useNewMetrics := !runner.featureGates[datalayer.EnableLegacyMetricsFeatureGate]
	epf := runner.setupMetricsCollection(useNewMetrics, opts, pmc)

	namespace := resolvePoolNamespace(opts.PoolNamespace)
	poolName := opts.PoolName
	if poolName == "" {
		poolName = "epp"
	}
	pool := datalayer.NewEndpointPool(namespace, poolName)
	ds := datastore.NewDatastore(ctx, epf, int32(opts.ModelServerMetricsPort)).WithEndpointPool(pool)

	if _, err := runner.parseConfigurationPhaseTwo(ctx, rawConfig, ds); err != nil {
		return nil, nil, err
	}

	disc := k8sdiscovery.NewInferencePoolDiscoveryPlugin(opts.PoolName, namespace, opts.PoolGroup, opts.EnableLeaderElection)
	disc.SetDatastore(ds)

	return ds, disc, nil
}
