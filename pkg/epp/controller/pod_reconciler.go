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

package controller

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/datastore"
	fwkdiscovery "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/discovery"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	podutil "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/util/pod"
)

// PodReconciler watches Kubernetes pods and drives backend lifecycle through
// the Notifier interface, fully complying with the BackendDiscovery contract.
// Pool provides pool state (readiness gate, label selector, target ports).
type PodReconciler struct {
	client.Reader
	Pool     datastore.Datastore
	Notifier fwkdiscovery.Notifier
}

func (c *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !c.Pool.PoolHasSynced() {
		logger.V(logutil.TRACE).Info("Skipping reconciling Pod because the InferencePool is not available yet")
		return ctrl.Result{}, nil
	}

	logger.V(logutil.VERBOSE).Info("Pod being reconciled")

	pod := &corev1.Pod{}
	if err := c.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			c.deletePodEndpoints(ctx, req.Name, req.Namespace)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unable to get pod - %w", err)
	}

	c.reconcilePod(ctx, pod)
	return ctrl.Result{}, nil
}

func (c *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	filter := predicate.Funcs{
		CreateFunc: func(ce event.CreateEvent) bool {
			pod := ce.Object.(*corev1.Pod)
			return c.Pool.PoolLabelsMatch(pod.GetLabels())
		},
		UpdateFunc: func(updateEvt event.UpdateEvent) bool {
			oldPod := updateEvt.ObjectOld.(*corev1.Pod)
			newPod := updateEvt.ObjectNew.(*corev1.Pod)
			return c.Pool.PoolLabelsMatch(oldPod.GetLabels()) || c.Pool.PoolLabelsMatch(newPod.GetLabels())
		},
		DeleteFunc: func(de event.DeleteEvent) bool {
			pod := de.Object.(*corev1.Pod)
			return c.Pool.PoolLabelsMatch(pod.GetLabels())
		},
		GenericFunc: func(ge event.GenericEvent) bool {
			pod := ge.Object.(*corev1.Pod)
			return c.Pool.PoolLabelsMatch(pod.GetLabels())
		},
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(filter).
		Complete(c)
}

// reconcilePod upserts or deletes backend endpoints for the pod via the Notifier.
func (c *PodReconciler) reconcilePod(ctx context.Context, pod *corev1.Pod) {
	logger := log.FromContext(ctx)
	if !podutil.IsPodReady(pod) || !c.Pool.PoolLabelsMatch(pod.Labels) {
		logger.V(logutil.DEBUG).Info("Pod not ready or labels do not match, deleting endpoints")
		c.deletePodEndpoints(ctx, pod.Name, pod.Namespace)
		return
	}

	pool, err := c.Pool.PoolGet()
	if err != nil || pool == nil {
		return
	}

	labels := make(map[string]string, len(pod.GetLabels()))
	maps.Copy(labels, pod.GetLabels())

	activePorts := datastore.ExtractActivePorts(pod, pool.TargetPorts)
	upserted := 0
	for idx, port := range pool.TargetPorts {
		if !activePorts.Has(port) {
			continue
		}
		meta := &fwkdl.EndpointMetadata{
			NamespacedName: datastore.CreateEndpointNamespacedName(pod, idx),
			PodName:        pod.Name,
			Address:        pod.Status.PodIP,
			Port:           strconv.Itoa(port),
			MetricsHost:    net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port)),
			Labels:         labels,
		}
		c.Notifier.Upsert(meta)
		upserted++
	}

	if upserted == 0 {
		logger.V(logutil.VERBOSE).Info("No container ports match pool targetPorts, pod will not receive traffic",
			"pod", pod.Name, "namespace", pod.Namespace, "targetPorts", pool.TargetPorts)
	} else {
		logger.V(logutil.DEFAULT).Info("Pod backends upserted", "pod", pod.Name, "count", upserted)
	}
}

// deletePodEndpoints calls Notifier.Delete for every endpoint rank that could
// have been created for this pod under the current pool target ports.
func (c *PodReconciler) deletePodEndpoints(ctx context.Context, podName, namespace string) {
	pool, err := c.Pool.PoolGet()
	if err != nil || pool == nil {
		return
	}
	stub := &corev1.Pod{}
	stub.Name = podName
	stub.Namespace = namespace
	for idx := range pool.TargetPorts {
		c.Notifier.Delete(datastore.CreateEndpointNamespacedName(stub, idx))
	}
	log.FromContext(ctx).V(logutil.DEFAULT).Info("Pod backends deleted", "pod", podName)
}

// EndpointsForPod extracts the EndpointMetadata list for a ready pod given a pool.
// Exported for use in tests and the K8s discovery plugins.
func EndpointsForPod(pod *corev1.Pod, targetPorts []int) []*fwkdl.EndpointMetadata {
	labels := make(map[string]string, len(pod.GetLabels()))
	maps.Copy(labels, pod.GetLabels())

	activePorts := datastore.ExtractActivePorts(pod, targetPorts)
	var metas []*fwkdl.EndpointMetadata
	for idx, port := range targetPorts {
		if !activePorts.Has(port) {
			continue
		}
		metas = append(metas, &fwkdl.EndpointMetadata{
			NamespacedName: datastore.CreateEndpointNamespacedName(pod, idx),
			PodName:        pod.Name,
			Address:        pod.Status.PodIP,
			Port:           strconv.Itoa(port),
			MetricsHost:    net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port)),
			Labels:         labels,
		})
	}
	return metas
}

// IDs returns the expected endpoint NamespacedNames for a pod under the given target ports.
func EndpointIDsForPod(podName, namespace string, targetPortCount int) []types.NamespacedName {
	stub := &corev1.Pod{}
	stub.Name = podName
	stub.Namespace = namespace
	ids := make([]types.NamespacedName, targetPortCount)
	for idx := range targetPortCount {
		ids[idx] = datastore.CreateEndpointNamespacedName(stub, idx)
	}
	return ids
}
