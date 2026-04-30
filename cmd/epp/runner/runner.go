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
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	healthPb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	configapi "github.com/llm-d/llm-d-inference-scheduler/apix/config/v1alpha1"
	"github.com/llm-d/llm-d-inference-scheduler/internal/rungroup"
	"github.com/llm-d/llm-d-inference-scheduler/internal/runnable"
	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/tracing"
	backendmetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/backend/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/config"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/config/loader"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/flowcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/flowcontrol/contracts"
	fccontroller "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/flowcontrol/controller"
	fcregistry "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/flowcontrol/registry"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/requesthandling"
	attrconcurrency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	extractormetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	sourcemetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/source/metrics"
	sourcenotifications "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/source/notifications"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/fairness/globalstrict"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/fairness/roundrobin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/ordering/edf"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/ordering/fcfs"
	slodeadline "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/ordering/slodeadline"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/usagelimits"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/admitter/latencyslo"
	reqdataprodprefix "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/dataproducer/approximateprefix"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload"
	latencyproducer "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/dataproducer/predictedlatency"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/requestattributereporter"
	testresponsereceived "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/test/responsereceived"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requesthandling/parsers/passthrough"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requesthandling/parsers/vllmgrpc"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/sloheadroomtier"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/picker/random"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/picker/weightedrandom"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/profile"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/kvcacheutilization"
	latencyscorer "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/latency"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/loraaffinity"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/prefix"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/queuedepth"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/runningrequests"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/scorer/tokenload"
	testfilter "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/test/filter"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/handlers"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/metrics/collectors"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/requestcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/scheduling"
	runserver "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/server"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/util/env"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	discoveryfile "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/discovery/file"
	k8sdiscovery "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/discovery/k8s"
	"github.com/llm-d/llm-d-inference-scheduler/version"
)

const (
	// enableExperimentalFlowControlLayer defines the environment variable used as a feature flag for the pluggable flow
	// control layer.
	// DEPRECATION NOTICE - this env var will be removed in the next version as we switch to configuring the EPP using FeatureGates in the config file.
	enableExperimentalFlowControlLayer = "ENABLE_EXPERIMENTAL_FLOW_CONTROL_LAYER"
)

var (
	setupLog = ctrl.Log.WithName("setup")
)

// NewRunner initializes a new EPP Runner and returns its pointer.
func NewRunner() *Runner {
	return &Runner{
		eppExecutableName:    "GIE",
		requestControlConfig: requestcontrol.NewConfig(),
		customCollectors:     []prometheus.Collector{},
	}
}

// Runner is used to run epp with its plugins.
type Runner struct {
	eppExecutableName    string
	featureGates         map[string]bool
	requestControlConfig *requestcontrol.Config
	schedulerConfig      *scheduling.SchedulerConfig
	customCollectors     []prometheus.Collector
	parser               fwkrh.Parser
	dlRuntime            *datalayer.Runtime
	handle               fwkplugin.Handle
}

func (r *Runner) WithExecutableName(exeName string) *Runner {
	r.eppExecutableName = exeName
	return r
}

func (r *Runner) WithRequestControlConfig(requestControlConfig *requestcontrol.Config) *Runner {
	r.requestControlConfig = requestControlConfig
	return r
}

func (r *Runner) WithSchedulerConfig(schedulerConfig *scheduling.SchedulerConfig) *Runner {
	r.schedulerConfig = schedulerConfig
	return r
}

func (r *Runner) WithCustomCollectors(collectors ...prometheus.Collector) *Runner {
	r.customCollectors = collectors
	return r
}

// Run is the single unified execution path for all deployment modes.
// Discovery is fully delegated to the DiscoveryPlugin: K8s plugins own their
// ctrl.Manager internally; the runner never calls ctrl.GetConfig() directly.
func (r *Runner) Run(ctx context.Context) error {
	logutil.InitSetupLogging()
	setupLog.Info(r.eppExecutableName+" build", "commit-sha", version.CommitSHA, "build-ref", version.BuildRef)

	opts := runserver.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if err := opts.Complete(); err != nil {
		return err
	}
	if err := opts.Validate(); err != nil {
		setupLog.Error(err, "Failed to validate flags")
		return err
	}

	flags := make(map[string]any)
	pflag.VisitAll(func(f *pflag.Flag) { flags[f.Name] = f.Value })
	setupLog.Info("Flags processed", "flags", flags)

	logutil.InitLogging(&opts.ZapOptions)

	if opts.Tracing {
		if err := tracing.InitTracing(ctx, setupLog, "gateway-api-inference-extension/epp"); err != nil {
			return fmt.Errorf("failed to init tracing %w", err)
		}
	}

	rawConfig, err := r.parseConfigurationPhaseOne(ctx, opts)
	if err != nil {
		setupLog.Error(err, "Failed to parse configuration")
		return err
	}

	// Datastore -- no K8s dependency. K8s plugins set the pool via PoolSet/PoolSetStatic.
	namespace := resolvePoolNamespace(opts.PoolNamespace)
	poolName := opts.PoolName
	if poolName == "" {
		if v := os.Getenv("POD_NAME"); v != "" {
			poolName = v
		} else {
			poolName = "epp"
		}
	}
	useNewMetrics := !r.featureGates[datalayer.EnableLegacyMetricsFeatureGate]
	var pmc backendmetrics.PodMetricsClient
	if !useNewMetrics {
		pmc, err = backendmetrics.NewPodMetricsClientImpl(setupLog, backendmetrics.Config{
			ModelServerMetricsScheme:        opts.ModelServerMetricsScheme,
			ModelServerMetricsHTTPSInsecure: opts.ModelServerMetricsHTTPSInsecure,
			ModelServerMetricsPath:          opts.ModelServerMetricsPath,
			TotalQueuedRequestsMetric:       opts.TotalQueuedRequestsMetric,
			TotalRunningRequestsMetric:      opts.TotalRunningRequestsMetric,
			KVCacheUsagePercentageMetric:    opts.KVCacheUsagePercentageMetric,
			LoRAInfoMetric:                  opts.LoRAInfoMetric,
			CacheInfoMetric:                 opts.CacheInfoMetric,
		})
		if err != nil {
			return err
		}
	}
	epf := r.setupMetricsCollection(useNewMetrics, opts, pmc)
	pool := datalayer.NewEndpointPool(namespace, poolName)
	ds := datastore.NewDatastore(ctx, epf, int32(opts.ModelServerMetricsPort)).WithEndpointPool(pool)

	eppConfig, err := r.parseConfigurationPhaseTwo(ctx, rawConfig, ds)
	if err != nil {
		setupLog.Error(err, "Failed to parse configuration")
		return err
	}

	disc, err := r.resolveDiscovery(rawConfig, opts, namespace, ds)
	if err != nil {
		setupLog.Error(err, "Failed to resolve discovery plugin")
		return err
	}

	// Configure datalayer for HTTP metrics polling (no manager binding).
	disallowedExtractorType := ""
	if !useNewMetrics {
		disallowedExtractorType = extractormetrics.MetricsExtractorType
	}
	if err := r.dlRuntime.Configure(eppConfig.DataConfig, useNewMetrics, disallowedExtractorType, setupLog); err != nil {
		setupLog.Error(err, "Failed to configure datalayer")
		return err
	}

	if r.schedulerConfig == nil {
		return errors.New("scheduler config must be set either by config api or through code")
	}
	setupLog.Info("parsed config", "scheduler-config", r.schedulerConfig)

	scheduler := scheduling.NewSchedulerWithConfig(r.schedulerConfig)
	endpointCandidates := requestcontrol.NewDatastoreEndpointCandidates(ds,
		requestcontrol.WithDisableEndpointSubsetFilter(opts.DisableEndpointSubsetFilter))

	var admissionController requestcontrol.AdmissionController
	var epCandidates contracts.EndpointCandidates = endpointCandidates
	if r.featureGates[flowcontrol.FeatureGate] {
		epCandidates = requestcontrol.NewCachedEndpointCandidates(ctx, endpointCandidates, time.Millisecond*50)
		setupLog.Info("Initializing experimental Flow Control layer")
		registry, err := fcregistry.NewFlowRegistry(eppConfig.FlowControlConfig.Registry, setupLog)
		if err != nil {
			return fmt.Errorf("failed to initialize Flow Registry: %w", err)
		}
		fc, err := fccontroller.NewFlowController(ctx, opts.PoolName, eppConfig.FlowControlConfig.Controller,
			fccontroller.Deps{
				Registry:           registry,
				SaturationDetector: eppConfig.SaturationDetector,
				EndpointCandidates: epCandidates,
				UsageLimitPolicy:   eppConfig.FlowControlConfig.UsageLimitPolicy,
			})
		if err != nil {
			return fmt.Errorf("failed to initialize Flow Controller: %w", err)
		}
		go registry.Run(ctx)
		admissionController = requestcontrol.NewFlowControlAdmissionController(fc, opts.PoolName)
	} else {
		setupLog.Info("Experimental Flow Control layer is disabled, using legacy admission control")
		admissionController = requestcontrol.NewLegacyAdmissionController(eppConfig.SaturationDetector, epCandidates)
	}

	director := requestcontrol.NewDirectorWithConfig(ds, scheduler, admissionController, epCandidates, r.requestControlConfig)
	useExperimental := r.featureGates[datalayer.ExperimentalDatalayerFeatureGate] || !r.featureGates[datalayer.EnableLegacyMetricsFeatureGate]
	serverRunner := r.buildServerRunner(opts, ds, director, eppConfig, useExperimental)
	r.registerMetrics(ds)

	setupLog.Info("EPP starting",
		"grpcPort", opts.GRPCPort,
		"healthPort", opts.GRPCHealthPort,
		"metricsPort", opts.MetricsPort,
		"pool", poolName,
		"namespace", namespace,
		"discoveryPlugin", rawConfig.Discovery)

	g := rungroup.New()
	g.Add("discovery", func(ctx context.Context) error { return disc.Start(ctx, discovery.NewNotifier(ds)) })
	g.Add("pollers", r.dlRuntime.StartPollers)
	r.addCommonRunnables(g, opts, ds, serverRunner)
	return g.Run(ctx)
}

// resolveDiscovery returns the DiscoveryPlugin to use. If a discovery section is
// present in the config it uses the referenced plugin; otherwise it synthesizes
// a K8s plugin from CLI flags for backward compatibility.
func (r *Runner) resolveDiscovery(rawConfig *configapi.EndpointPickerConfig, opts *runserver.Options, namespace string, ds datastore.Datastore) (discovery.DiscoveryPlugin, error) {
	if rawConfig.Discovery != nil {
		raw := r.handle.Plugin(rawConfig.Discovery.PluginRef)
		if raw == nil {
			return nil, fmt.Errorf("discovery pluginRef %q not found", rawConfig.Discovery.PluginRef)
		}
		disc, ok := raw.(discovery.DiscoveryPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %q does not implement DiscoveryPlugin", rawConfig.Discovery.PluginRef)
		}
		if dp, ok := raw.(k8sdiscovery.DatastoreProvider); ok {
			dp.SetDatastore(ds)
		}
		return disc, nil
	}

	// Backward compat: synthesize from CLI flags.
	if opts.EndpointSelector != "" {
		p := k8sdiscovery.NewStaticSelectorDiscoveryPlugin(opts.EndpointSelector, namespace, opts.EndpointTargetPorts)
		p.SetDatastore(ds)
		return p, nil
	}
	if opts.PoolName == "" {
		return nil, errors.New("--pool-name is required when no discovery plugin is configured")
	}
	p := k8sdiscovery.NewInferencePoolDiscoveryPlugin(opts.PoolName, namespace, opts.PoolGroup, opts.EnableLeaderElection)
	p.SetDatastore(ds)
	return p, nil
}

// registerInTreePlugins registers the factory functions of all known plugins.
func (r *Runner) registerInTreePlugins() {
	fwkplugin.Register(prefix.PrefixCacheScorerPluginType, prefix.PrefixCachePluginFactory)
	fwkplugin.Register(maxscore.MaxScorePickerType, maxscore.MaxScorePickerFactory)
	fwkplugin.Register(random.RandomPickerType, random.RandomPickerFactory)
	fwkplugin.Register(weightedrandom.WeightedRandomPickerType, weightedrandom.WeightedRandomPickerFactory)
	fwkplugin.Register(profile.SingleProfileHandlerType, profile.SingleProfileHandlerFactory)
	fwkplugin.Register(kvcacheutilization.KvCacheUtilizationScorerType, kvcacheutilization.KvCacheUtilizationScorerFactory)
	fwkplugin.Register(queuedepth.QueueScorerType, queuedepth.QueueScorerFactory)
	fwkplugin.Register(runningrequests.RunningRequestsSizeScorerType, runningrequests.RunningRequestsSizeScorerFactory)
	fwkplugin.Register(loraaffinity.LoraAffinityScorerType, loraaffinity.LoraAffinityScorerFactory)
	fwkplugin.Register(tokenload.TokenLoadScorerType, tokenload.TokenLoadScorerFactory)
	// Flow Control plugins
	fwkplugin.Register(globalstrict.GlobalStrictFairnessPolicyType, globalstrict.GlobalStrictFairnessPolicyFactory)
	fwkplugin.Register(roundrobin.RoundRobinFairnessPolicyType, roundrobin.RoundRobinFairnessPolicyFactory)
	fwkplugin.Register(fcfs.FCFSOrderingPolicyType, fcfs.FCFSOrderingPolicyFactory)
	fwkplugin.Register(edf.EDFOrderingPolicyType, edf.EDFOrderingPolicyFactory)
	fwkplugin.Register(slodeadline.SLODeadlineOrderingPolicyType, slodeadline.SLODeadlineOrderingPolicyFactory)
	fwkplugin.Register(usagelimits.StaticUsageLimitPolicyType, usagelimits.StaticPolicyFactory)
	// Request level data producer plugins
	fwkplugin.RegisterAsDefaultProducer(reqdataprodprefix.ApproxPrefixCachePluginType, reqdataprodprefix.ApproxPrefixCacheFactory, attrprefix.PrefixCacheMatchInfoKey)
	fwkplugin.RegisterAsDefaultProducer(inflightload.InFlightLoadProducerType, inflightload.InFlightLoadProducerFactory, attrconcurrency.InFlightLoadKey)
	fwkplugin.RegisterAsDefaultProducer(latencyproducer.LatencyDataProviderPluginType, latencyproducer.PredictedLatencyFactory, attrlatency.LatencyPredictionInfoKey)
	// Latency predictor plugins
	fwkplugin.Register(latencyslo.LatencyAdmissionPluginType, latencyslo.LatencyAdmissionFactory)
	// Latency scoring and filtering plugins
	fwkplugin.Register(prefixcacheaffinity.PluginType, prefixcacheaffinity.Factory)
	fwkplugin.Register(sloheadroomtier.PluginType, sloheadroomtier.Factory)
	fwkplugin.Register(latencyscorer.LatencyScorerType, latencyscorer.Factory)
	// Test-only plugins
	fwkplugin.Register(testfilter.HeaderBasedTestingFilterType, testfilter.HeaderBasedTestingFilterFactory)
	fwkplugin.Register(testresponsereceived.DestinationEndpointServedVerifierType, testresponsereceived.DestinationEndpointServedVerifierFactory)
	// Datalayer plugins
	fwkplugin.Register(sourcemetrics.MetricsDataSourceType, sourcemetrics.MetricsDataSourceFactory)
	fwkplugin.Register(extractormetrics.MetricsExtractorType, extractormetrics.CoreMetricsExtractorFactory)
	fwkplugin.Register(sourcenotifications.NotificationSourceType, sourcenotifications.NotificationSourceFactory)
	fwkplugin.Register(sourcenotifications.EndpointNotificationSourceType, sourcenotifications.EndpointSourceFactory)
	// Request control plugins
	fwkplugin.Register(requestattributereporter.RequestAttributeReporterType, requestattributereporter.RequestAttributeReporterPluginFactory)
	fwkplugin.Register(openai.OpenAIParserType, openai.OpenAIParserPluginFactory)
	fwkplugin.Register(vllmgrpc.VllmGRPCParserType, vllmgrpc.VllmGRPCParserPluginFactory)
	fwkplugin.Register(passthrough.PassthroughParserType, passthrough.PassthroughParserPluginFactory)
	// Saturation detector plugins
	fwkplugin.Register(concurrency.ConcurrencyDetectorType, concurrency.ConcurrencyDetectorFactory)
	fwkplugin.Register(utilization.UtilizationDetectorType, utilization.UtilizationDetectorFactory)
	// Discovery plugins
	fwkplugin.Register(discoveryfile.PluginType, discoveryfile.Factory)
	fwkplugin.Register(k8sdiscovery.InferencePoolPluginType, k8sdiscovery.InferencePoolFactory)
	fwkplugin.Register(k8sdiscovery.StaticSelectorPluginType, k8sdiscovery.StaticSelectorFactory)
}

func (r *Runner) parseConfigurationPhaseOne(ctx context.Context, opts *runserver.Options) (*configapi.EndpointPickerConfig, error) {
	logger := log.FromContext(ctx)

	var configBytes []byte
	if opts.ConfigText != "" {
		configBytes = []byte(opts.ConfigText)
	} else if opts.ConfigFile != "" {
		var err error
		configBytes, err = os.ReadFile(opts.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load config from a file '%s' - %w", opts.ConfigFile, err)
		}
	}

	loader.RegisterFeatureGate(datalayer.ExperimentalDatalayerFeatureGate)
	loader.RegisterFeatureGate(datalayer.EnableLegacyMetricsFeatureGate)
	loader.RegisterFeatureGate(flowcontrol.FeatureGate)

	r.registerInTreePlugins()

	rawConfig, featureGates, err := loader.LoadRawConfig(configBytes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config - %w", err)
	}
	r.featureGates = featureGates

	if r.featureGates[datalayer.ExperimentalDatalayerFeatureGate] {
		setupLog.Info("The data layer is now enabled by default. " +
			"Please remove the 'dataLayer' feature gate from your config. " +
			"To fall back to legacy metrics polling, use the 'enableLegacyMetrics' feature gate.")
	}
	if r.featureGates[datalayer.EnableLegacyMetricsFeatureGate] {
		setupLog.Info("Data layer: using legacy metrics polling (opt-in via 'enableLegacyMetrics' feature gate)")
	} else {
		setupLog.Info("Data layer: ENABLED (default)")
	}

	return rawConfig, nil
}

func makePodListFunc(ds datastore.Datastore) func() []types.NamespacedName {
	return func() []types.NamespacedName {
		pods := ds.PodList(datastore.AllPodsPredicate)
		names := make([]types.NamespacedName, 0, len(pods))
		for _, p := range pods {
			names = append(names, p.GetMetadata().NamespacedName)
		}
		return names
	}
}

func (r *Runner) parseConfigurationPhaseTwo(ctx context.Context, rawConfig *configapi.EndpointPickerConfig, ds datastore.Datastore) (*config.Config, error) {
	logger := log.FromContext(ctx)

	applyDeprecatedEnvFeatureGate(enableExperimentalFlowControlLayer, "Flow Control layer", flowcontrol.FeatureGate, rawConfig)

	handle := fwkplugin.NewEppHandle(ctx, makePodListFunc(ds))
	r.handle = handle
	cfg, err := loader.InstantiateAndConfigure(rawConfig, handle, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load the configuration - %w", err)
	}

	r.schedulerConfig = cfg.SchedulerConfig
	r.requestControlConfig.AddPlugins(handle.GetAllPlugins()...)

	dataProducers, err := datalayer.CreateMissingDataProducers(handle.GetAllPlugins(), fwkplugin.DefaultProducerRegistry, fwkplugin.Registry, handle)
	if err != nil {
		return nil, fmt.Errorf("failed to create missing data producers - %w", err)
	}
	for _, p := range dataProducers {
		handle.AddPlugin(p.TypedName().Name, p)
	}
	r.requestControlConfig.AddPlugins(dataProducers...)

	dag, err := datalayer.ValidateAndOrderDataDependencies(handle.GetAllPlugins())
	if err != nil {
		return nil, fmt.Errorf("failed to load the configuration - %w", err)
	}
	r.requestControlConfig.OrderPrepareDataPlugins(dag)

	r.parser = handlers.NewParser(cfg.ParserConfig)
	logger.Info("loaded configuration from file/text successfully")
	return cfg, nil
}

func applyDeprecatedEnvFeatureGate(envVar, featureName, featureGate string, rawConfig *configapi.EndpointPickerConfig) {
	if _, ok := os.LookupEnv(envVar); ok {
		setupLog.Info(fmt.Sprintf("Enabling the experimental %s using environment variables is deprecated and will be removed in next version", featureName))
		if env.GetEnvBool(envVar, false, setupLog) {
			if rawConfig.FeatureGates == nil {
				rawConfig.FeatureGates = make(configapi.FeatureGates, 0)
			}
			rawConfig.FeatureGates = append(rawConfig.FeatureGates, featureGate)
		}
	}
}

func (r *Runner) setupMetricsCollection(enableNewMetrics bool, opts *runserver.Options, pmc backendmetrics.PodMetricsClient) datalayer.EndpointFactory {
	r.dlRuntime = datalayer.NewRuntime(opts.RefreshMetricsInterval)
	if enableNewMetrics {
		return r.dlRuntime
	}
	return backendmetrics.NewPodMetricsFactory(pmc, opts.RefreshMetricsInterval)
}

func (r *Runner) registerMetrics(ds datastore.Datastore) {
	r.customCollectors = append(r.customCollectors, collectors.NewInferencePoolMetricsCollector(ds))
	metrics.Register(r.customCollectors...)
	metrics.RecordInferenceExtensionInfo(version.CommitSHA, version.BuildRef)
}

func (r *Runner) buildServerRunner(opts *runserver.Options, ds datastore.Datastore, director *requestcontrol.Director, eppConfig *config.Config, useExperimentalDatalayer bool) *runserver.ExtProcServerRunner {
	return &runserver.ExtProcServerRunner{
		GrpcPort:                         opts.GRPCPort,
		Datastore:                        ds,
		SecureServing:                    opts.SecureServing,
		HealthChecking:                   opts.HealthChecking,
		CertPath:                         opts.CertPath,
		EnableCertReload:                 opts.EnableCertReload,
		RefreshPrometheusMetricsInterval: opts.RefreshPrometheusMetricsInterval,
		MetricsStalenessThreshold:        opts.MetricsStalenessThreshold,
		Director:                         director,
		Parser:                           r.parser,
		SaturationDetector:               eppConfig.SaturationDetector,
		UseExperimentalDatalayerV2:       useExperimentalDatalayer,
	}
}

func (r *Runner) addCommonRunnables(g rungroup.RunnableGroup, opts *runserver.Options, ds datastore.Datastore, serverRunner *runserver.ExtProcServerRunner) {
	logger := ctrl.Log.WithName("ext-proc")
	g.Add("ext-proc", func(ctx context.Context) error {
		return serverRunner.AsRunnable(logger).Start(ctx)
	})
	g.Add("health", buildHealthServer(ds, opts.GRPCHealthPort, r.parser))
	g.Add("metrics", func(ctx context.Context) error { return serveMetrics(ctx, opts.MetricsPort) })
}

func buildHealthServer(ds datastore.Datastore, port int, supporter appProtocolSupporter) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		logger := ctrl.Log.WithName("health")
		isLeader := &atomic.Bool{}
		isLeader.Store(true)
		srv := grpc.NewServer()
		healthPb.RegisterHealthServer(srv, &healthServer{
			logger:                logger,
			datastore:             ds,
			isLeader:              isLeader,
			leaderElectionEnabled: false,
			supporter:             supporter,
		})
		return runnable.GRPCServer("health", srv, port).Start(ctx)
	}
}

func serveMetrics(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics server error: %w", err)
	}
	return nil
}

func resolvePoolNamespace(poolNamespace string) string {
	if poolNamespace != "" {
		return poolNamespace
	}
	if nsEnv := os.Getenv("NAMESPACE"); nsEnv != "" {
		return nsEnv
	}
	return runserver.DefaultPoolNamespace
}
