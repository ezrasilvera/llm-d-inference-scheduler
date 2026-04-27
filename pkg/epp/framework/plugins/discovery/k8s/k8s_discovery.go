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

// Package k8s provides BackendDiscovery plugins that discover inference backends
// by watching Kubernetes pods. Both plugins create and own their ctrl.Manager
// internally — the runner does not call ctrl.GetConfig() or ctrl.NewManager().
//
// Available plugins:
//
//   - inference-pool-backend-discovery: selector and ports come from an InferencePool
//     CRD. Also starts InferenceObjective and InferenceModelRewrite reconcilers.
//
//   - static-selector-backend-discovery: selector and ports come from plugin parameters.
//     No InferencePool CRD required.
package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8sdiscovery "k8s.io/client-go/discovery"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	"sigs.k8s.io/gateway-api-inference-extension/apix/v1alpha2"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/common"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/controller"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	fwkdiscovery "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const (
	// InferencePoolPluginType is the plugin type for K8s discovery via InferencePool CRD.
	InferencePoolPluginType = "inference-pool-backend-discovery"
	// StaticSelectorPluginType is the plugin type for K8s discovery with a static selector.
	StaticSelectorPluginType = "static-selector-backend-discovery"
)

var pluginScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(pluginScheme))
	utilruntime.Must(v1alpha2.Install(pluginScheme))
	utilruntime.Must(v1.Install(pluginScheme))
}

// DatastoreProvider is an optional interface K8s discovery plugins implement to
// receive the shared Datastore before Start() is called. The runner calls
// SetDatastore() after plugin instantiation and before the errgroup starts.
type DatastoreProvider interface {
	SetDatastore(ds datastore.Datastore)
}

// --- inference-pool-backend-discovery ---

type inferencePoolParams struct {
	PoolName       string `json:"poolName"`
	PoolNamespace  string `json:"poolNamespace"`
	PoolGroup      string `json:"poolGroup"`
	LeaderElection bool   `json:"leaderElection"`
}

// InferencePoolDiscovery discovers backends via an InferencePool CRD.
type InferencePoolDiscovery struct {
	typedName     fwkplugin.TypedName
	PoolName      string
	PoolNamespace string
	PoolGroup     string
	LeaderElect   bool
	ds            datastore.Datastore
}

var _ fwkdiscovery.BackendDiscovery = (*InferencePoolDiscovery)(nil)
var _ fwkplugin.Plugin = (*InferencePoolDiscovery)(nil)
var _ DatastoreProvider = (*InferencePoolDiscovery)(nil)

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
	return &InferencePoolDiscovery{
		typedName:     fwkplugin.TypedName{Type: InferencePoolPluginType, Name: name},
		PoolName:      p.PoolName,
		PoolNamespace: p.PoolNamespace,
		PoolGroup:     p.PoolGroup,
		LeaderElect:   p.LeaderElection,
	}, nil
}

func (k *InferencePoolDiscovery) TypedName() fwkplugin.TypedName { return k.typedName }
func (k *InferencePoolDiscovery) SetDatastore(ds datastore.Datastore) { k.ds = ds }

// Start creates a ctrl.Manager, registers all relevant reconcilers, and blocks
// until ctx is cancelled.
func (k *InferencePoolDiscovery) Start(ctx context.Context, notifier fwkdiscovery.Notifier) error {
	if k.ds == nil {
		return errors.New("inference-pool-backend-discovery: datastore not set; call SetDatastore before Start")
	}

	if k.PoolName == "" {
		return errors.New("inference-pool-backend-discovery: poolName must be set in plugin parameters or via --pool-name flag")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("inference-pool-backend-discovery: failed to get K8s REST config: %w", err)
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
		return fmt.Errorf("inference-pool-backend-discovery: failed to create discovery client: %w", err)
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
		return fmt.Errorf("inference-pool-backend-discovery: failed to create manager: %w", err)
	}

	if err := (&controller.InferencePoolReconciler{
		Datastore: k.ds, Reader: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("inference-pool-backend-discovery: InferencePoolReconciler: %w", err)
	}
	if err := (&controller.PodReconciler{
		Pool:     k.ds,
		Notifier: notifier,
		Reader:   mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("inference-pool-backend-discovery: PodReconciler: %w", err)
	}
	if hasObjective {
		if err := (&controller.InferenceObjectiveReconciler{
			Datastore: k.ds, Reader: mgr.GetClient(), PoolGKNN: gknn,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("inference-pool-backend-discovery: InferenceObjectiveReconciler: %w", err)
		}
	}
	if hasModelRewrite {
		if err := (&controller.InferenceModelRewriteReconciler{
			Datastore: k.ds, Reader: mgr.GetClient(), PoolGKNN: gknn,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("inference-pool-backend-discovery: InferenceModelRewriteReconciler: %w", err)
		}
	}

	return mgr.Start(ctx)
}

// --- static-selector-backend-discovery ---

type staticSelectorParams struct {
	EndpointSelector    string `json:"endpointSelector"`
	EndpointTargetPorts []int  `json:"endpointTargetPorts"`
	Namespace           string `json:"namespace"`
}

// StaticSelectorDiscovery discovers backends by watching pods matching a fixed
// label selector from plugin parameters.
type StaticSelectorDiscovery struct {
	typedName           fwkplugin.TypedName
	EndpointSelector    string
	EndpointTargetPorts []int
	Namespace           string
	ds                  datastore.Datastore
}

var _ fwkdiscovery.BackendDiscovery = (*StaticSelectorDiscovery)(nil)
var _ fwkplugin.Plugin = (*StaticSelectorDiscovery)(nil)
var _ DatastoreProvider = (*StaticSelectorDiscovery)(nil)

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
	return &StaticSelectorDiscovery{
		typedName:           fwkplugin.TypedName{Type: StaticSelectorPluginType, Name: name},
		EndpointSelector:    p.EndpointSelector,
		EndpointTargetPorts: p.EndpointTargetPorts,
		Namespace:           p.Namespace,
	}, nil
}

func (s *StaticSelectorDiscovery) TypedName() fwkplugin.TypedName { return s.typedName }
func (s *StaticSelectorDiscovery) SetDatastore(ds datastore.Datastore) { s.ds = ds }

// Start pre-populates the pool from parameters, creates a ctrl.Manager,
// registers a PodReconciler, and blocks until ctx is cancelled.
func (s *StaticSelectorDiscovery) Start(ctx context.Context, notifier fwkdiscovery.Notifier) error {
	if s.ds == nil {
		return errors.New("static-selector-backend-discovery: datastore not set; call SetDatastore before Start")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("static-selector-backend-discovery: failed to get K8s REST config: %w", err)
	}

	selectorMap, err := labels.ConvertSelectorToLabelsMap(s.EndpointSelector)
	if err != nil {
		return fmt.Errorf("static-selector-backend-discovery: invalid endpointSelector: %w", err)
	}

	// Pre-populate pool so PodReconciler can use PoolLabelsMatch immediately.
	pool := datalayer.NewEndpointPool(s.Namespace, "static-selector")
	pool.Selector = selectorMap
	pool.TargetPorts = s.EndpointTargetPorts
	s.ds.PoolSetStatic(pool)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  pluginScheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return fmt.Errorf("static-selector-backend-discovery: failed to create manager: %w", err)
	}

	if err := (&controller.PodReconciler{
		Pool:     s.ds,
		Notifier: notifier,
		Reader:   mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("static-selector-backend-discovery: PodReconciler: %w", err)
	}

	return mgr.Start(ctx)
}

// gvkInstalled returns true if the given GVK is registered in the cluster.
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
