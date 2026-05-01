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

// Package k8s provides EndpointDiscovery implementations that discover inference
// endpoints by watching Kubernetes pods. Both plugins own their ctrl.Manager
// internally -- the runner does not call ctrl.GetConfig() or ctrl.NewManager().
//
// Available plugins:
//
//   - inference-pool-discovery: selector and ports come from an InferencePool CRD.
//     Also starts InferenceObjective and InferenceModelRewrite reconcilers when
//     those CRDs are installed.
//
//   - static-selector-discovery: selector and ports come from plugin parameters.
//     No InferencePool CRD required.
package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sdiscovery "k8s.io/client-go/discovery"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	"sigs.k8s.io/gateway-api-inference-extension/apix/v1alpha2"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/common"
	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/controller"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	podutil "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/util/pod"
)

const (
	// InferencePoolPluginType is the plugin type for K8s discovery via InferencePool CRD.
	InferencePoolPluginType = "inference-pool-discovery"
	// StaticSelectorPluginType is the plugin type for K8s discovery via a static label selector.
	StaticSelectorPluginType = "static-selector-discovery"

	activePortsAnnotation = "inference.networking.k8s.io/active-ports"
)

var pluginScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(pluginScheme))
	utilruntime.Must(v1alpha2.Install(pluginScheme))
	utilruntime.Must(v1.Install(pluginScheme))
}

// DatastoreProvider is implemented by K8s discovery plugins that require the
// shared Datastore. The runner calls SetDatastore after plugin instantiation
// and before starting the errgroup.
type DatastoreProvider interface {
	SetDatastore(ds datastore.Datastore)
}

// ---- shared pod-to-endpoint helpers ----------------------------------------

// podToEndpoints extracts EndpointMetadata entries from a ready pod using the
// pool's target ports. One entry is produced per active port.
func podToEndpoints(pod *corev1.Pod, targetPorts []int) []*fwkdl.EndpointMetadata {
	activePorts := extractActivePorts(pod, targetPorts)
	metas := make([]*fwkdl.EndpointMetadata, 0, len(targetPorts))
	for idx, port := range targetPorts {
		if !activePorts[port] {
			continue
		}
		metas = append(metas, &fwkdl.EndpointMetadata{
			NamespacedName: endpointID(pod.Name, pod.Namespace, idx),
			PodName:        pod.Name,
			Address:        pod.Status.PodIP,
			Port:           strconv.Itoa(port),
			MetricsHost:    net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port)),
			Labels:         pod.GetLabels(),
		})
	}
	return metas
}

// endpointID returns the NamespacedName for a pod endpoint at the given port index.
func endpointID(podName, namespace string, idx int) types.NamespacedName {
	return types.NamespacedName{
		Name:      podName + "-rank-" + strconv.Itoa(idx),
		Namespace: namespace,
	}
}

// allEndpointIDs returns all potential endpoint IDs for a pod across all target ports.
// Used for deletion when the pod object is no longer available.
func allEndpointIDs(podName, namespace string, targetPorts []int) []types.NamespacedName {
	ids := make([]types.NamespacedName, len(targetPorts))
	for idx := range targetPorts {
		ids[idx] = endpointID(podName, namespace, idx)
	}
	return ids
}

func extractActivePorts(pod *corev1.Pod, targetPorts []int) map[int]bool {
	all := make(map[int]bool, len(targetPorts))
	for _, p := range targetPorts {
		all[p] = true
	}
	annotation, ok := pod.GetAnnotations()[activePortsAnnotation]
	if !ok {
		return all
	}
	active := make(map[int]bool)
	for _, s := range strings.Split(annotation, ",") {
		var p int
		if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &p); err == nil && all[p] {
			active[p] = true
		}
	}
	return active
}

// ---- podNotifierReconciler -------------------------------------------------

// podNotifierReconciler is an internal controller that translates pod events
// into discovery.Notifier calls. It is used by both K8s discovery plugins.
type podNotifierReconciler struct {
	client.Reader
	ds       datastore.Datastore
	notifier discovery.Notifier
	ports    func() []int // returns current target ports; may change after pool reconcile
	mu       sync.Mutex
}

func (r *podNotifierReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !r.ds.PoolHasSynced() {
		logger.V(logutil.TRACE).Info("Skipping pod reconcile: pool not yet synced")
		return ctrl.Result{}, nil
	}

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			for _, id := range allEndpointIDs(req.Name, req.Namespace, r.ports()) {
				r.notifier.Delete(id)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to get pod: %w", err)
	}

	if !podutil.IsPodReady(pod) || !r.ds.PoolLabelsMatch(pod.Labels) {
		for _, id := range allEndpointIDs(pod.Name, pod.Namespace, r.ports()) {
			r.notifier.Delete(id)
		}
		return ctrl.Result{}, nil
	}

	for _, meta := range podToEndpoints(pod, r.ports()) {
		r.notifier.Upsert(meta)
	}
	return ctrl.Result{}, nil
}

func (r *podNotifierReconciler) SetupWithManager(mgr ctrl.Manager) error {
	filter := predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return r.ds.PoolLabelsMatch(e.Object.GetLabels()) },
		UpdateFunc:  func(e event.UpdateEvent) bool {
			return r.ds.PoolLabelsMatch(e.ObjectOld.GetLabels()) || r.ds.PoolLabelsMatch(e.ObjectNew.GetLabels())
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return r.ds.PoolLabelsMatch(e.Object.GetLabels()) },
		GenericFunc: func(e event.GenericEvent) bool { return r.ds.PoolLabelsMatch(e.Object.GetLabels()) },
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(filter).
		Complete(r)
}

// ---- InferencePoolDiscoveryPlugin ------------------------------------------

type inferencePoolParams struct {
	PoolName       string `json:"poolName"`
	PoolNamespace  string `json:"poolNamespace"`
	PoolGroup      string `json:"poolGroup"`
	LeaderElection bool   `json:"leaderElection"`
}

// InferencePoolDiscoveryPlugin discovers endpoints via an InferencePool CRD.
// It creates and owns its own ctrl.Manager internally.
type InferencePoolDiscoveryPlugin struct {
	typedName     fwkplugin.TypedName
	PoolName      string
	PoolNamespace string
	PoolGroup     string
	LeaderElect   bool
	ds            datastore.Datastore
}

var _ discovery.EndpointDiscovery = (*InferencePoolDiscoveryPlugin)(nil)
var _ fwkplugin.Plugin = (*InferencePoolDiscoveryPlugin)(nil)
var _ DatastoreProvider = (*InferencePoolDiscoveryPlugin)(nil)

// NewInferencePoolDiscoveryPlugin creates a plugin instance directly (without the factory),
// for use in the runner's backward-compat path when no discovery config is present.
func NewInferencePoolDiscoveryPlugin(poolName, poolNamespace, poolGroup string, leaderElect bool) *InferencePoolDiscoveryPlugin {
	return &InferencePoolDiscoveryPlugin{
		typedName:     fwkplugin.TypedName{Type: InferencePoolPluginType, Name: InferencePoolPluginType},
		PoolName:      poolName,
		PoolNamespace: poolNamespace,
		PoolGroup:     poolGroup,
		LeaderElect:   leaderElect,
	}
}

// InferencePoolFactory is the plugin factory for inference-pool-discovery.
func InferencePoolFactory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &inferencePoolParams{PoolGroup: "inference.networking.k8s.io"}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("%s: failed to parse parameters: %w", InferencePoolPluginType, err)
		}
	}
	if name == "" {
		name = InferencePoolPluginType
	}
	return &InferencePoolDiscoveryPlugin{
		typedName:     fwkplugin.TypedName{Type: InferencePoolPluginType, Name: name},
		PoolName:      p.PoolName,
		PoolNamespace: p.PoolNamespace,
		PoolGroup:     p.PoolGroup,
		LeaderElect:   p.LeaderElection,
	}, nil
}

func (k *InferencePoolDiscoveryPlugin) TypedName() fwkplugin.TypedName { return k.typedName }
func (k *InferencePoolDiscoveryPlugin) SetDatastore(ds datastore.Datastore) { k.ds = ds }

// Start creates a ctrl.Manager, registers reconcilers, and blocks until ctx is cancelled.
func (k *InferencePoolDiscoveryPlugin) Start(ctx context.Context, notifier discovery.Notifier) error {
	if k.ds == nil {
		return errors.New("inference-pool-discovery: datastore not set; call SetDatastore before Start")
	}
	if k.PoolName == "" {
		return errors.New("inference-pool-discovery: poolName is required (set in plugin parameters or via --pool-name)")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("inference-pool-discovery: failed to get K8s REST config: %w", err)
	}

	namespace := k.PoolNamespace
	if namespace == "" {
		namespace = "default"
	}

	gknn := common.GKNN{
		NamespacedName: types.NamespacedName{Name: k.PoolName, Namespace: namespace},
		GroupKind:      schema.GroupKind{Group: k.PoolGroup, Kind: "InferencePool"},
	}

	dc, err := k8sdiscovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("inference-pool-discovery: failed to create discovery client: %w", err)
	}
	hasObjective := gvkInstalled(dc, v1alpha2.GroupVersion.Group, v1alpha2.GroupVersion.Version, "InferenceObjective")
	hasModelRewrite := gvkInstalled(dc, v1alpha2.GroupVersion.Group, v1alpha2.GroupVersion.Version, "InferenceModelRewrite")

	cacheOpts := cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {Namespaces: map[string]cache.Config{namespace: {}}},
			&v1.InferencePool{}: {Namespaces: map[string]cache.Config{namespace: {
				FieldSelector: fields.SelectorFromSet(fields.Set{"metadata.name": k.PoolName}),
			}}},
		},
	}
	if hasObjective {
		cacheOpts.ByObject[&v1alpha2.InferenceObjective{}] = cache.ByObject{
			Namespaces: map[string]cache.Config{namespace: {}},
		}
	}
	if hasModelRewrite {
		cacheOpts.ByObject[&v1alpha2.InferenceModelRewrite{}] = cache.ByObject{
			Namespaces: map[string]cache.Config{namespace: {}},
		}
	}

	mgrOpts := ctrl.Options{
		Scheme:  pluginScheme,
		Cache:   cacheOpts,
		Metrics: metricsserver.Options{BindAddress: "0"},
	}
	if k.LeaderElect {
		mgrOpts.LeaderElection = true
		mgrOpts.LeaderElectionResourceLock = "leases"
		mgrOpts.LeaderElectionID = fmt.Sprintf("epp-%s-%s.inference-pool-discovery", namespace, k.PoolName)
		mgrOpts.LeaderElectionNamespace = namespace
		mgrOpts.LeaderElectionReleaseOnCancel = true
	}

	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		return fmt.Errorf("inference-pool-discovery: failed to create manager: %w", err)
	}

	if err := (&controller.InferencePoolReconciler{
		Datastore: k.ds,
		Reader:    mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("inference-pool-discovery: InferencePoolReconciler: %w", err)
	}

	podRec := &podNotifierReconciler{
		Reader:   mgr.GetClient(),
		ds:       k.ds,
		notifier: notifier,
		ports:    func() []int { return poolTargetPorts(k.ds) },
	}
	if err := podRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("inference-pool-discovery: PodReconciler: %w", err)
	}

	if hasObjective {
		if err := (&controller.InferenceObjectiveReconciler{
			Datastore: k.ds,
			Reader:    mgr.GetClient(),
			PoolGKNN:  gknn,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("inference-pool-discovery: InferenceObjectiveReconciler: %w", err)
		}
	}
	if hasModelRewrite {
		if err := (&controller.InferenceModelRewriteReconciler{
			Datastore: k.ds,
			Reader:    mgr.GetClient(),
			PoolGKNN:  gknn,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("inference-pool-discovery: InferenceModelRewriteReconciler: %w", err)
		}
	}

	return mgr.Start(ctx)
}

// ---- StaticSelectorDiscoveryPlugin -----------------------------------------

type staticSelectorParams struct {
	EndpointSelector    string `json:"endpointSelector"`
	EndpointTargetPorts []int  `json:"endpointTargetPorts"`
	Namespace           string `json:"namespace"`
}

// StaticSelectorDiscoveryPlugin discovers endpoints by watching pods matching a
// fixed label selector from plugin parameters. No InferencePool CRD required.
type StaticSelectorDiscoveryPlugin struct {
	typedName    fwkplugin.TypedName
	selector     string
	targetPorts  []int
	namespace    string
	ds           datastore.Datastore
}

var _ discovery.EndpointDiscovery = (*StaticSelectorDiscoveryPlugin)(nil)
var _ fwkplugin.Plugin = (*StaticSelectorDiscoveryPlugin)(nil)
var _ DatastoreProvider = (*StaticSelectorDiscoveryPlugin)(nil)

// NewStaticSelectorDiscoveryPlugin creates a plugin instance directly for the
// runner's backward-compat path when --endpoint-selector is set on the CLI.
func NewStaticSelectorDiscoveryPlugin(selector, namespace string, targetPorts []int) *StaticSelectorDiscoveryPlugin {
	return &StaticSelectorDiscoveryPlugin{
		typedName:   fwkplugin.TypedName{Type: StaticSelectorPluginType, Name: StaticSelectorPluginType},
		selector:    selector,
		targetPorts: targetPorts,
		namespace:   namespace,
	}
}

// StaticSelectorFactory is the plugin factory for static-selector-discovery.
func StaticSelectorFactory(name string, parameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	p := &staticSelectorParams{Namespace: "default"}
	if len(parameters) > 0 {
		if err := json.Unmarshal(parameters, p); err != nil {
			return nil, fmt.Errorf("%s: failed to parse parameters: %w", StaticSelectorPluginType, err)
		}
	}
	if p.EndpointSelector == "" {
		return nil, fmt.Errorf("%s: 'endpointSelector' parameter is required", StaticSelectorPluginType)
	}
	if len(p.EndpointTargetPorts) == 0 {
		return nil, fmt.Errorf("%s: 'endpointTargetPorts' parameter is required", StaticSelectorPluginType)
	}
	if name == "" {
		name = StaticSelectorPluginType
	}
	return &StaticSelectorDiscoveryPlugin{
		typedName:   fwkplugin.TypedName{Type: StaticSelectorPluginType, Name: name},
		selector:    p.EndpointSelector,
		targetPorts: p.EndpointTargetPorts,
		namespace:   p.Namespace,
	}, nil
}

func (s *StaticSelectorDiscoveryPlugin) TypedName() fwkplugin.TypedName { return s.typedName }
func (s *StaticSelectorDiscoveryPlugin) SetDatastore(ds datastore.Datastore) { s.ds = ds }

// Start pre-populates the pool from parameters, then creates a ctrl.Manager,
// registers a pod reconciler, and blocks until ctx is cancelled.
func (s *StaticSelectorDiscoveryPlugin) Start(ctx context.Context, notifier discovery.Notifier) error {
	if s.ds == nil {
		return errors.New("static-selector-discovery: datastore not set; call SetDatastore before Start")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("static-selector-discovery: failed to get K8s REST config: %w", err)
	}

	selectorMap, err := labels.ConvertSelectorToLabelsMap(s.selector)
	if err != nil {
		return fmt.Errorf("static-selector-discovery: invalid endpointSelector: %w", err)
	}

	pool := datalayer.NewEndpointPool(s.namespace, StaticSelectorPluginType)
	pool.Selector = selectorMap
	pool.TargetPorts = s.targetPorts
	s.ds.PoolSetStatic(pool)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  pluginScheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return fmt.Errorf("static-selector-discovery: failed to create manager: %w", err)
	}

	podRec := &podNotifierReconciler{
		Reader:   mgr.GetClient(),
		ds:       s.ds,
		notifier: notifier,
		ports:    func() []int { return s.targetPorts },
	}
	if err := podRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("static-selector-discovery: PodReconciler: %w", err)
	}

	return mgr.Start(ctx)
}

// ---- helpers ---------------------------------------------------------------

func poolTargetPorts(ds datastore.Datastore) []int {
	pool, err := ds.PoolGet()
	if err != nil || pool == nil {
		return nil
	}
	return pool.TargetPorts
}

func gvkInstalled(dc k8sdiscovery.DiscoveryInterface, group, version, kind string) bool {
	list, err := dc.ServerResourcesForGroupVersion(group + "/" + version)
	if err != nil {
		return false
	}
	for _, r := range list.APIResources {
		if r.Kind == kind {
			return true
		}
	}
	return false
}
