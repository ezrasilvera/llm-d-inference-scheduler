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

// Package runner contains the standalone (non-K8s) EPP runner.
// It starts the EPP without any Kubernetes API server dependency.
package runner

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/config/loader"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery"
	discoverydns "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery/dns"
	discoveryfile "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery/file"
	discoverygrpc "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery/grpc"
	discoveryhttp "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/discovery/http"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/handlers"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/metrics/collectors"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/requestcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/scheduling"
	runserver "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/server"
	"github.com/llm-d/llm-d-inference-scheduler/version"
)

var setupLog = ctrl.Log.WithName("setup")

// Run is the entrypoint for the standalone EPP.
func Run(ctx context.Context) error {
	logutil.InitSetupLogging()
	setupLog.Info("standalone EPP build", "commit-sha", version.CommitSHA, "build-ref", version.BuildRef)

	opts := runserver.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if err := opts.Complete(); err != nil {
		return err
	}

	logutil.InitLogging(&opts.ZapOptions)
	logger := log.FromContext(ctx)

	// Register discovery plugins before any config loading.
	registerDiscoveryPlugins()
	loader.RegisterFeatureGate(datalayer.ExperimentalDatalayerFeatureGate)
	loader.RegisterFeatureGate(datalayer.EnableLegacyMetricsFeatureGate)

	// Load raw config bytes.
	var configBytes []byte
	switch {
	case opts.ConfigText != "":
		configBytes = []byte(opts.ConfigText)
	case opts.ConfigFile != "":
		var err error
		configBytes, err = os.ReadFile(opts.ConfigFile)
		if err != nil {
			return fmt.Errorf("failed to load config from %s: %w", opts.ConfigFile, err)
		}
	}

	rawConfig, featureGates, err := loader.LoadRawConfig(configBytes, logger)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	if rawConfig.BackendDiscovery == nil {
		return fmt.Errorf("standalone mode requires backendDiscovery to be configured in the config file")
	}

	// Build the endpoint pool. In standalone mode the selector is not used for pod
	// filtering — BackendDiscovery provides backends directly. The pool object is still
	// needed for pool identity (name, namespace).
	namespace := resolveNamespace(opts.PoolNamespace)
	poolName := resolvePoolName(opts)
	pool := datalayer.NewEndpointPool(namespace, poolName)

	// Create datalayer runtime and datastore.
	dlRuntime := datalayer.NewRuntime(opts.RefreshMetricsInterval)
	ds := datastore.NewDatastoreWithPool(ctx, dlRuntime, int32(opts.ModelServerMetricsPort), pool)

	// Seed static objectives and model rewrites before plugin instantiation.
	if rawConfig.StaticConfig != nil {
		for i := range rawConfig.StaticConfig.Objectives {
			ds.ObjectiveSet(&rawConfig.StaticConfig.Objectives[i])
		}
		for i := range rawConfig.StaticConfig.ModelRewrites {
			ds.ModelRewriteSet(&rawConfig.StaticConfig.ModelRewrites[i])
		}
		logger.Info("loaded static config",
			"objectives", len(rawConfig.StaticConfig.Objectives),
			"modelRewrites", len(rawConfig.StaticConfig.ModelRewrites))
	}

	// Build a no-op PodList func — BackendDiscovery provides endpoints.
	podListFn := func() []types.NamespacedName {
		pods := ds.PodList(datastore.AllPodsPredicate)
		names := make([]types.NamespacedName, 0, len(pods))
		for _, p := range pods {
			names = append(names, p.GetMetadata().NamespacedName)
		}
		return names
	}
	handle := fwkplugin.NewEppHandle(ctx, podListFn)

	eppConfig, err := loader.InstantiateAndConfigure(rawConfig, handle, logger)
	if err != nil {
		return fmt.Errorf("failed to instantiate config: %w", err)
	}

	// Guard: fail fast if any K8s notification sources are configured.
	for _, p := range handle.GetAllPlugins() {
		if _, ok := p.(fwkdl.NotificationSource); ok {
			return fmt.Errorf("plugin %q uses k8s-notification-source which is not supported in standalone mode", p.TypedName())
		}
	}

	// Ensure exactly one BackendDiscovery plugin.
	var discoveryCount int
	for _, p := range handle.GetAllPlugins() {
		if _, ok := p.(discovery.BackendDiscovery); ok {
			discoveryCount++
		}
	}
	if discoveryCount > 1 {
		return fmt.Errorf("exactly one BackendDiscovery plugin is allowed, found %d", discoveryCount)
	}

	// Resolve the BackendDiscovery plugin instance.
	rawDisc := handle.Plugin(rawConfig.BackendDiscovery.PluginRef)
	if rawDisc == nil {
		return fmt.Errorf("backendDiscovery pluginRef %q not found", rawConfig.BackendDiscovery.PluginRef)
	}
	disc, ok := rawDisc.(discovery.BackendDiscovery)
	if !ok {
		return fmt.Errorf("plugin %q does not implement BackendDiscovery", rawConfig.BackendDiscovery.PluginRef)
	}

	// Configure datalayer runtime for HTTP metrics polling.
	useNewMetrics := !featureGates[datalayer.EnableLegacyMetricsFeatureGate]
	if err := dlRuntime.Configure(eppConfig.DataConfig, useNewMetrics, "", setupLog); err != nil {
		return fmt.Errorf("failed to configure datalayer: %w", err)
	}

	// Build scheduling and request control components.
	scheduler := scheduling.NewSchedulerWithConfig(eppConfig.SchedulerConfig)
	endpointCandidates := requestcontrol.NewDatastoreEndpointCandidates(ds)
	admissionController := requestcontrol.NewLegacyAdmissionController(eppConfig.SaturationDetector, endpointCandidates)
	reqCtrlConfig := requestcontrol.NewConfig()
	reqCtrlConfig.AddPlugins(handle.GetAllPlugins()...)
	director := requestcontrol.NewDirectorWithConfig(ds, scheduler, admissionController, endpointCandidates, reqCtrlConfig)
	parser := handlers.NewParser(eppConfig.ParserConfig)

	// Register Prometheus metrics.
	metrics.Register(collectors.NewInferencePoolMetricsCollector(ds))
	metrics.RecordInferenceExtensionInfo(version.CommitSHA, version.BuildRef)

	// Build gRPC servers.
	extProcSrv := grpc.NewServer()
	extProcPb.RegisterExternalProcessorServer(extProcSrv, handlers.NewStreamingServer(ds, director, parser))

	healthSrv := grpc.NewServer()
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(healthSrv, healthcheck)
	svcName := extProcPb.ExternalProcessor_ServiceDesc.ServiceName
	healthcheck.SetServingStatus(svcName, healthgrpc.HealthCheckResponse_SERVING)

	setupLog.Info("standalone EPP starting",
		"grpcPort", opts.GRPCPort,
		"healthPort", opts.GRPCHealthPort,
		"metricsPort", opts.MetricsPort)

	// Run all components under a shared errgroup for clean lifecycle management.
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return disc.Start(gctx, &datastoreNotifier{ds: ds})
	})
	g.Go(func() error {
		return dlRuntime.StartPollers(gctx)
	})
	g.Go(func() error {
		return serveGRPC(gctx, extProcSrv, opts.GRPCPort, "ext-proc")
	})
	g.Go(func() error {
		return serveGRPC(gctx, healthSrv, opts.GRPCHealthPort, "health")
	})
	g.Go(func() error {
		return serveMetrics(gctx, opts.MetricsPort)
	})

	return g.Wait()
}

// datastoreNotifier adapts the datastore.Datastore to the discovery.Notifier interface.
type datastoreNotifier struct {
	ds datastore.Datastore
}

func (n *datastoreNotifier) Upsert(meta *fwkdl.EndpointMetadata) {
	n.ds.BackendUpsert(context.Background(), meta)
}

func (n *datastoreNotifier) Delete(id types.NamespacedName) {
	n.ds.BackendDelete(id)
}

func (n *datastoreNotifier) MarkSynced() {
	n.ds.MarkDiscoverySynced()
}

// serveGRPC starts a gRPC server and blocks until ctx is cancelled.
func serveGRPC(ctx context.Context, srv *grpc.Server, port int, name string) error {
	logger := ctrl.Log.WithValues("name", name, "port", port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("%s gRPC server failed to listen: %w", name, err)
	}
	logger.Info("gRPC server listening")
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			logger.Info("gRPC server shutting down")
			srv.GracefulStop()
		case <-done:
		}
	}()
	if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("%s gRPC server error: %w", name, err)
	}
	return nil
}

// serveMetrics starts a plain HTTP Prometheus metrics server.
func serveMetrics(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background()) //nolint:errcheck
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics server error: %w", err)
	}
	return nil
}

// registerDiscoveryPlugins registers all built-in BackendDiscovery plugin factories.
func registerDiscoveryPlugins() {
	fwkplugin.Register(discoveryfile.PluginType, discoveryfile.Factory)
	fwkplugin.Register(discoveryhttp.PluginType, discoveryhttp.Factory)
	fwkplugin.Register(discoverygrpc.PluginType, discoverygrpc.Factory)
	fwkplugin.Register(discoverydns.PluginType, discoverydns.Factory)
}

func resolveNamespace(ns string) string {
	if ns != "" {
		return ns
	}
	if env := os.Getenv("NAMESPACE"); env != "" {
		return env
	}
	return runserver.DefaultPoolNamespace
}

func resolvePoolName(opts *runserver.Options) string {
	if opts.PoolName != "" {
		return opts.PoolName
	}
	if env := os.Getenv("POD_NAME"); env != "" {
		return env
	}
	return "standalone-epp"
}
