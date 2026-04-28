# Discovery Plugin Architecture

## Overview

The EPP discovers inference endpoints through a **DiscoveryPlugin** — a pluggable abstraction that populates and maintains the endpoint datastore independently of the underlying infrastructure. By default the EPP uses Kubernetes CRD reconcilers to discover endpoints. When a `DiscoveryPlugin` is configured, all Kubernetes reconcilers are bypassed and the plugin becomes the sole source of truth for endpoint lifecycle.

This enables the EPP to run without a Kubernetes cluster, which is valuable for RL training and inference workloads on non-Kubernetes infrastructure such as **Slurm** and **Ray** clusters.

---

## Core Interfaces

### `DiscoveryPlugin`

```go
type DiscoveryPlugin interface {
    Start(ctx context.Context, notifier Notifier) error
}
```

`Start` is the plugin's entry point. It must:
1. Enumerate all currently known endpoints and call `notifier.Upsert` with the full batch.
2. Continue watching for changes until `ctx` is cancelled.
3. Block until `ctx` is cancelled or a fatal error occurs.

### `Notifier`

```go
type Notifier interface {
    Upsert(endpoints []*EndpointMetadata)
    Delete(id types.NamespacedName)
}
```

The `Notifier` is the callback interface through which the plugin drives the datastore.

**Ordering contract:** the datastore processes `Upsert` and `Delete` calls in the order they are received. Plugin implementations **must** preserve event order — do not buffer, coalesce, or dispatch calls concurrently in a way that could reorder them. For example, an `Upsert` followed by a `Delete` for the same endpoint must arrive in that order, or the endpoint will be incorrectly left in the datastore.

---

## Plugin Registration

A `DiscoveryPlugin` implementation must also implement `fwkplugin.Plugin` so it can be registered in the plugin registry and referenced by name from the EPP config.

```go
fwkplugin.Register(myDiscovery.PluginType, myDiscovery.Factory)
```

---

## Configuration

To enable a discovery plugin, add a `discovery` section to `EndpointPickerConfig` referencing the plugin by name:

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - name: my-discovery
    type: file-discovery
    parameters:
      path: /etc/epp/endpoints.yaml
      watchFile: true
discovery:
  pluginRef: my-discovery
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: random-picker
```

When a `discovery` field is present and the referenced plugin is of type `file-discovery`, the EPP starts in **no-kube mode**: the Kubernetes controller manager and all CRD/pod reconcilers are not started. The EPP runs as a standalone process, requiring no cluster connectivity.

---

## Built-in Plugins

### `file-discovery`

Reads a YAML (or JSON) file listing inference endpoints. Optionally watches the file for changes at runtime using `fsnotify`.

**Package:** `pkg/epp/framework/plugins/discovery/file`

**Plugin type:** `file-discovery`

#### Parameters

| Parameter   | Type   | Required | Default | Description |
|-------------|--------|----------|---------|-------------|
| `path`      | string | yes      | —       | Path to the endpoints file |
| `watchFile` | bool   | no       | `false` | If true, reload the file when it changes on disk |

#### Endpoints file format

The file is YAML (JSON is also accepted). Top-level key is `endpoints`, a list of endpoint entries.

```yaml
endpoints:
  - name: vllm-0
    namespace: default       # optional, defaults to "default"
    address: 10.0.0.1
    port: "8080"
    metricsHost: 10.0.0.1:8080   # optional; derived as address:port if omitted
    labels:
      model: llama-3-8b
      gpu: h100

  - name: vllm-1
    address: 10.0.0.2
    port: "8080"
    labels:
      model: llama-3-8b
      gpu: h100
```

#### Reload behaviour

When `watchFile: true`, the plugin watches the file using `fsnotify`. On any `Write` or `Create` event:
- All endpoints present in the new file are upserted (add or update).
- Endpoints that were in the previous file but are absent from the new one are deleted.
- Events are always delivered in file order: upserts first, then deletes.

When `watchFile: false` (the default), the file is loaded once at startup and the plugin blocks until the context is cancelled. Use this for static deployments where the endpoint list is fixed at launch time.

#### Full configuration example

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - name: file-discovery
    type: file-discovery
    parameters:
      path: /etc/epp/endpoints.yaml
      watchFile: true

  - name: random-picker
    type: random-picker

discovery:
  pluginRef: file-discovery

schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: random-picker

dataLayer:
  sources:
    - pluginRef: metrics-data-source
      extractors:
        - pluginRef: core-metrics-extractor
```

#### Deployment without Kubernetes

When using `file-discovery`, the EPP does not connect to a Kubernetes API server. You can run it as a plain process or container alongside your Slurm/Ray workload:

```bash
epp \
  --config-file /etc/epp/config.yaml \
  --pool-name my-pool \
  --pool-namespace default \
  --grpc-port 9002 \
  --grpc-health-port 9003 \
  --metrics-port 9090
```

The endpoints file can be written by an external process — for example, a Slurm epilog script that writes the allocated node addresses, or a Ray cluster initialisation hook.

---

## Implementing a Custom Discovery Plugin

1. Create a struct that implements both `discovery.DiscoveryPlugin` and `fwkplugin.Plugin`.
2. Write a factory function with signature `func(name string, parameters json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error)`.
3. Register the factory: `fwkplugin.Register(MyPluginType, MyFactory)`.
4. Reference it in `EndpointPickerConfig` via the `discovery.pluginRef` field.

The `file-discovery` plugin at `pkg/epp/framework/plugins/discovery/file/file_discovery.go` serves as a reference implementation.
