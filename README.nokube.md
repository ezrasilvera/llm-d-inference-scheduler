# llm-d Inference Scheduler: nokube Mode

## Architecture

The discovery plugin architecture is documented in
[`docs/images/discovery-arch.drawio`](docs/images/discovery-arch.drawio)
(open with [diagrams.net](https://app.diagrams.net)).

```
┌─────────────────────────────────────────────────────────────────────────┐
│  EPP Process  (single binary, single runner)                            │
│                                                                         │
│  ┌─────────── Runner — errgroup ──────────────────────────────────┐    │
│  │  disc.Start()  dlRuntime.StartPollers()  serveGRPC  Metrics    │    │
│  └─────────────────────┬──────────────────────────────────────────┘    │
│                         │                                               │
│          ┌──────────────▼──────────────┐                               │
│          │   BackendDiscovery interface │                               │
│          │   Start(ctx, Notifier) error │                               │
│          └──────┬──────────────┬───────┘                               │
│                 │               │                                       │
│    ┌────────────▼──────┐  ┌────▼────────────────────────────────────┐  │
│    │  Non-K8s plugins  │  │  K8s plugins (own ctrl.Manager)         │  │
│    │  (no API server)  │  │                                          │  │
│    │                   │  │  inference-pool-backend-discovery        │  │
│    │  file-backend-    │  │   ctrl.Manager owns:                    │  │
│    │  discovery        │  │    InferencePoolReconciler               │  │
│    │                   │  │    PodReconciler                         │  │
│    │  dns-backend-     │  │    InferenceObjectiveReconciler (opt)    │  │
│    │  discovery        │  │    InferenceModelRewriteReconciler (opt) │  │
│    │                   │  │                                          │  │
│    └─────────┬─────────┘  │  static-selector-backend-discovery      │  │
│              │             │   ctrl.Manager owns:                    │  │
│              │             │    PodReconciler                         │  │
│              │             └──────────────────┬───────────────────────┘  │
│              │                                │                          │
│              └────────────────┬───────────────┘                          │
│                               │ Notifier (Upsert / Delete / MarkSynced)  │
│                       ┌───────▼────────────────────┐                    │
│                       │         Datastore           │                    │
│                       │  endpoints | pool | obj     │                    │
│                       │  rewrites                   │                    │
│                       └───────────┬─────────────────┘                   │
│                                   │                                      │
│                       ┌───────────▼─────────────────┐                   │
│                       │   Director / Scheduler       │                   │
└───────────────────────────────────────────────────────────────────────────┘
```

## Overview

The inference scheduler (EPP) discovers backends via a `BackendDiscovery` plugin. All
four plugins ship in the same binary and image. The plugin type in the config determines
whether a Kubernetes API server is required at runtime.

Key differences by plugin type:

| Capability | K8s plugins (inference-pool, static-selector) | Non-K8s plugins (file, dns) |
|---|---|---|
| Backend discovery | Pod watch via K8s API — owns `ctrl.Manager` internally | File or DNS polling — no K8s dependency |
| Pool configuration | Reconciled live from `InferencePool` CRD | CLI flags at startup |
| `InferenceObjective` | Reconciled live (inference-pool only) | Static via `staticConfig` |
| `InferenceModelRewrite` | Reconciled live (inference-pool only) | Static via `staticConfig` |
| Lifecycle inside plugin | `ctrl.Manager` | N/A — runner errgroup handles everything |
| HA / leader election | Supported (inference-pool, `leaderElection: true`) | Not supported |

Scheduling algorithms, flow control, metrics polling from inference engines, Prometheus
metrics, TLS, and health checking all work identically regardless of discovery plugin.

---

## Design options: Level 1 vs Level 2

<details>
<summary>Show</summary>

Two levels of Kubernetes decoupling are possible.

> ### Selected approach: Level 1
> The current codebase follows **Level 1** and will be fully implemented as a single
> binary when that work is undertaken. Level 2 is documented here as a possible future
> hardening option. It is **not** the current or planned implementation.

### Level 1: runtime independence only (current approach)

The binary compiles with K8s Go packages. Whether a K8s API server is contacted at
runtime depends entirely on which `BackendDiscovery` plugin is configured:

- **Non-K8s plugins** (file, dns): `ctrl.GetConfig()` and `ctrl.NewManager()` are never
  called. No reconcilers start. The runner manages everything with `errgroup`.
- **K8s plugins** (inference-pool, static-selector): `ctrl.GetConfig()` and
  `ctrl.NewManager()` are called **inside the plugin's `Start()`** method. Reconcilers
  run inside that internal manager. The runner's errgroup just calls `disc.Start()` and
  the plugin handles all K8s lifecycle internally.

The runner itself is always a single errgroup path. There is no K8s-specific code in the
runner's `Run()` function.

Code structure:
```
cmd/epp/runner/runner.go
  runErrgroup() / runNonK8s()  <- single path for all plugins (~90 lines)

cmd/epp/main.go  <- single entry point, mode auto-detected from config
```

### Level 2: full import isolation

The binary has no K8s package imports at all. It is built from a fully separate entry
point (separate `cmd/`) whose import graph excludes `k8s.io/api/core/v1`,
`sigs.k8s.io/controller-runtime`, and the K8s client.

This requires a fully independent runner (~310 lines) that re-implements lifecycle
management, gRPC server startup, and Prometheus metrics serving. All of that logic already
exists in the K8s runner, resulting in ~185 lines of duplication.

Code size comparison:

| | Level 1 (current) | Level 2 |
|---|---|---|
| `cmd/epp/runner/runner.go` | ~850 lines (both paths) | 731 lines |
| Separate nokube runner/binary | none | 310-line independent runner |
| Total runner code | ~850 lines | ~1041 lines |
| Duplicated setup block | 0 | ~185 lines |

### Comparison

| Property | Level 1 | Level 2 |
|---|---|---|
| Runs without K8s cluster at runtime | yes | yes |
| No K8s package imports in binary | no | yes |
| Binary size | ~10 MB larger | smaller |
| Supply-chain / SBOM cleanliness | K8s packages present | clean |
| Code duplication | 0 | ~185 lines |
| Separate binary/image | no | yes |

Level 1 gives a single binary with no duplication. Level 2 gives a cleaner import
graph at the cost of a separate binary and ~185 duplicated lines.

</details>

---

## Single image vs two images

<details>
<summary>Show</summary>

**Single image:** one image that runs in both K8s and non-K8s modes, selected by the
`backendDiscovery` plugin type in the config file. One artifact to build, sign, and push.
Mode is transparent from the config.

**Two images:** separate `epp` (K8s) and a dedicated non-K8s image built from separate
Dockerfiles. Each image does one thing; the name is self-documenting. Requires two tag
streams and two CI jobs.

### Selected option: single image

The current implementation uses a single `epp` image. Mode is determined entirely by
configuration:

- Non-K8s backends configured (`file-backend-discovery`, `dns-backend-discovery`)
  and `backendDiscovery` set in config: non-K8s execution path, no API server contact.
- K8s backends configured (`inference-pool-backend-discovery`,
  `static-selector-backend-discovery`) or no `backendDiscovery` at all: K8s execution
  path, full cluster integration.

If Level 2 import isolation becomes a requirement in the future, the two-image model
can be adopted without changing the config format or discovery plugin interfaces.

</details>

---

## BackendDiscovery abstraction

<details>
<summary>Show</summary>

### Interface

```go
// BackendDiscovery discovers inference backends and drives their lifecycle in the datastore.
// Implementations also implement fwkplugin.Plugin so they can be selected via
// EndpointPickerConfig.backendDiscovery.pluginRef.
type BackendDiscovery interface {
    Start(ctx context.Context, notifier Notifier) error
}

// Notifier is the callback through which BackendDiscovery communicates backend state.
type Notifier interface {
    // Upsert adds or updates a backend in the datastore.
    Upsert(meta *EndpointMetadata)
    // Delete removes a backend by its namespaced name.
    Delete(id types.NamespacedName)
    // MarkSynced signals that the initial discovery pass is complete,
    // unblocking health/readiness checks.
    MarkSynced()
}
```

`BackendDiscovery` plugins call the notifier; `datastoreNotifier` in
`cmd/epp/runner/runner.go` is the concrete implementation that bridges notifier calls to
the datastore (`BackendUpsert`, `BackendDelete`, `MarkDiscoverySynced`).

### EndpointMetadata: the backend descriptor

Each discovered backend is described by an `EndpointMetadata`:

| Field | Description |
|---|---|
| `NamespacedName` | Stable identity; unique key for upsert/delete |
| `PodName` | Human-readable name (can equal `NamespacedName.Name`) |
| `Address` | IP address or hostname the inference engine listens on |
| `Port` | Inference port (e.g. `"8000"`) |
| `MetricsHost` | `host:port` for Prometheus metrics scraping; defaults to `Address:Port` |
| `Labels` | Key-value labels used by scheduling filters (e.g. role, model name) |

### Discovery lifecycle contract

1. `Start` is called once and blocks until `ctx` is cancelled.
2. On startup, the implementation calls `Upsert` for every known backend, then calls
   `MarkSynced`. The EPP health check is not ready until `MarkSynced` is called.
3. After the initial sync, the implementation continues watching and calls `Upsert` or
   `Delete` as backends appear or disappear.
4. Exactly one `BackendDiscovery` plugin may be configured; the loader enforces this.

### Selecting a plugin via config

`BackendDiscovery` uses the same two-level plugin pattern as `Parser`, `SaturationDetector`,
and `DataLayer`:

1. Declare a plugin instance in the top-level `plugins` list with a type, name, and parameters.
2. Reference it by name in the `backendDiscovery.pluginRef` field.

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: <backend-discovery-type>
    name: my-discovery
    parameters:
      # plugin-specific parameters

backendDiscovery:
  pluginRef: my-discovery
```

</details>

---

## Implemented backends

<details>
<summary>Show</summary>

### 1. File-based (`file-backend-discovery`)

Reads a YAML or JSON file listing backends. Optionally watches the file for live updates.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | Path to the backends YAML/JSON file |
| `watchFile` | bool | `false` | When `true`, reload the file on every write event (via `fsnotify`) |

**Backends file format:**

```yaml
backends:
  - name: vllm-0
    namespace: default          # optional, defaults to "default"
    address: "10.0.0.1"
    port: "8000"
    metricsHost: "10.0.0.1:9090"  # optional, defaults to address:port
    labels:
      model: llama3
      role: decode
  - name: vllm-1
    address: "10.0.0.2"
    port: "8000"
```

**Config example:**

```yaml
plugins:
  - type: file-backend-discovery
    name: file-discovery
    parameters:
      path: /etc/epp/backends.yaml
      watchFile: true

backendDiscovery:
  pluginRef: file-discovery
```

### 2. DNS (`dns-backend-discovery`)

Resolves DNS records on a polling interval and treats each resolved address as a backend.
Supports SRV records (preferred) and A/AAAA records.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dnsMode` | `"srv"` or `"a"` | `"srv"` | Resolution strategy |
| `service` | string | required (srv) | Service name for SRV query |
| `proto` | string | `"tcp"` | Protocol for SRV query |
| `domain` | string | required (srv) | Domain suffix for SRV query |
| `host` | string | required (a) | Hostname for A/AAAA lookup |
| `port` | string | required (a) | Port to assign to all resolved addresses |
| `refreshInterval` | duration (nanoseconds) | 30s | Poll interval |
| `namespace` | string | `"default"` | Namespace assigned to discovered backends |
| `labels` | map[string]string | `{}` | Static labels attached to all discovered backends |

**SRV mode** queries `_<service>._<proto>.<domain>`:

```yaml
plugins:
  - type: dns-backend-discovery
    name: dns-discovery
    parameters:
      dnsMode: srv
      service: vllm
      proto: tcp
      domain: inference.svc.cluster.local
      refreshInterval: 30000000000    # 30s
      namespace: default
      labels:
        model: llama3

backendDiscovery:
  pluginRef: dns-discovery
```

**A record mode** queries a hostname directly:

```yaml
plugins:
  - type: dns-backend-discovery
    name: dns-discovery
    parameters:
      dnsMode: a
      host: vllm.inference.svc.cluster.local
      port: "8000"
      refreshInterval: 30000000000
      labels:
        role: prefill

backendDiscovery:
  pluginRef: dns-discovery
```

### 3. K8s InferencePool (`inference-pool-backend-discovery`)

Discovers backends by watching Kubernetes pods whose label selector and target ports are
defined by an `InferencePool` CRD. This is the standard K8s mode. The runner starts the
full set of CRD reconcilers: `InferencePool`, `InferenceObjective`, `InferenceModelRewrite`,
and `Pod`.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `poolName` | string | from `--pool-name` flag | Name of the `InferencePool` resource |
| `poolNamespace` | string | from `--pool-namespace` flag | Namespace of the `InferencePool` resource |
| `poolGroup` | string | `"inference.networking.k8s.io"` | API group of the `InferencePool` CRD |
| `leaderElection` | bool | `false` | Enable K8s leader election for HA |

**Config example:**

```yaml
plugins:
  - type: inference-pool-backend-discovery
    name: k8s-discovery
    parameters:
      poolName: my-pool
      poolNamespace: default

backendDiscovery:
  pluginRef: k8s-discovery
```

If `backendDiscovery` is absent from the config entirely, the runner defaults to
`inference-pool-backend-discovery` using the `--pool-name` and `--pool-namespace` CLI flags
for backward compatibility with existing deployments.

### 4. K8s static selector (`static-selector-backend-discovery`)

Discovers backends by watching Kubernetes pods matching a label selector from plugin
parameters. No `InferencePool` CRD is required. Useful when deploying the EPP alongside
vLLM pods without the full Gateway API Inference Extension CRD stack.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpointSelector` | string | required | Label selector for vLLM pods (e.g. `"app=vllm"`) |
| `endpointTargetPorts` | []int | required | Target ports on matched pods |
| `namespace` | string | `"default"` | Namespace to watch pods in |

**Config example:**

```yaml
plugins:
  - type: static-selector-backend-discovery
    name: static-k8s-discovery
    parameters:
      endpointSelector: "app=vllm,env=prod"
      endpointTargetPorts: [8000]
      namespace: inference

backendDiscovery:
  pluginRef: static-k8s-discovery
```

</details>

---

## Static configuration for objectives and model rewrites

<details>
<summary>Show</summary>

For `file-backend-discovery` and `dns-backend-discovery`, `InferenceObjective` and
`InferenceModelRewrite` are loaded once at startup from the `staticConfig` section of the
config file. For `inference-pool-backend-discovery`, these are reconciled live from their
K8s CRDs when present; `staticConfig` is still applied first and acts as a fallback if
the CRDs are not installed.

Both fields are optional:
- Without `InferenceObjective`, all requests use priority `0`.
- Without `InferenceModelRewrite`, model names pass through unchanged.

```yaml
staticConfig:
  objectives:
    - metadata:
        name: high-priority
      spec:
        priority: 10
    - metadata:
        name: batch
      spec:
        priority: -5

  modelRewrites:
    - metadata:
        name: llama-aliases
      spec:
        rules:
          - matches:
              - model:
                  value: "llama3"
            targets:
              - model: "meta-llama/Llama-3-8b-instruct"
```

The `spec.poolRef` field that these CRDs normally require is ignored in nokube mode.

</details>

---

## Capability comparison: K8s mode vs nokube mode

<details>
<summary>Show</summary>

### Fully supported

- All scheduling plugins (scorers, filters, pickers, profile handlers, disaggregated P/D)
- Flow control and admission control
- Request parsing (OpenAI, vllm gRPC, passthrough)
- DataLayer HTTP metrics polling from inference engines
- `endpoint-notification-source` (driven by BackendDiscovery lifecycle events, not K8s)
- `precise-prefix-cache-scorer` with ZMQ KV events
- Prometheus metrics (plain HTTP server instead of controller-runtime metrics server)
- TLS / secure serving
- Health checking

### Varies by plugin

| Feature | inference-pool | static-selector | file / dns |
|---|---|---|---|
| Backend discovery | Live pod watch (K8s) | Live pod watch (K8s) | File or DNS polling |
| Pool configuration | Live from `InferencePool` CRD | From plugin parameters | CLI flags |
| InferenceObjective / priority | Live CRD reconciliation | `staticConfig` only | `staticConfig` only |
| InferenceModelRewrite | Live CRD reconciliation | `staticConfig` only | `staticConfig` only |
| HA / leader election | Yes (`leaderElection: true`) | No | No |
| K8s API server required | Yes | Yes | No |

### Not available in any mode

| Feature | Reason |
|---|---|
| Dynamic pool reconfiguration at runtime | Pool is fixed at plugin startup |
| `k8s-notification-source` datalayer plugins | Not supported; config validation fails fast |
| Controller-runtime internal metrics | Not exposed (runner's Prometheus server handles metrics) |

</details>

---

## Full config example (file-based discovery)

<details>
<summary>Show</summary>

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig

plugins:
  - type: file-backend-discovery
    name: my-discovery
    parameters:
      path: /etc/epp/backends.yaml
      watchFile: true
  - type: queue-depth-scorer
  - type: kv-cache-utilization-scorer
  - type: prefix-cache-scorer

schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: queue-depth-scorer
        weight: 2.0
      - pluginRef: kv-cache-utilization-scorer
        weight: 2.0
      - pluginRef: prefix-cache-scorer
        weight: 3.0

dataLayer:
  sources:
    - pluginRef: metrics-data-source
      extractors:
        - pluginRef: metrics-extractor

backendDiscovery:
  pluginRef: my-discovery

staticConfig:
  objectives:
    - metadata:
        name: interactive
      spec:
        priority: 10
```

</details>

---

## Running the EPP

<details>
<summary>Show</summary>

```bash
docker run --rm \
  -v /path/to/config.yaml:/etc/epp/config.yaml \
  -v /path/to/backends.yaml:/etc/epp/backends.yaml \
  -p 9002:9002 -p 9003:9003 -p 9090:9090 \
  ghcr.io/llm-d/llm-d-inference-scheduler:dev \
  --config-file /etc/epp/config.yaml \
  --pool-name my-pool \
  --pool-namespace default \
  --secure-serving=false
```

</details>

---

## Appendix A: where to run vLLM

<details>
<summary>Show</summary>

The EPP is agnostic to how vLLM is deployed. It only needs an IP address, a port,
and a `/metrics` endpoint to scrape. The choice of deployment model affects only
which `BackendDiscovery` plugin you use.

| vLLM deployment | Recommended discovery plugin |
|---|---|
| Kubernetes pods with InferencePool CRD | `inference-pool-backend-discovery` |
| Kubernetes pods without InferencePool CRD | `static-selector-backend-discovery` |
| Docker containers | `file-backend-discovery` (shared volume) |
| Bare-metal processes | `file-backend-discovery` (registration script) |
| Slurm job steps | `file-backend-discovery` (shared filesystem) |
| Any environment with DNS SRV records | `dns-backend-discovery` |

### K8s pods

Standard deployment. In K8s mode, the `PodReconciler` watches pods matching the
`InferencePool` label selector and registers them automatically. No configuration
needed beyond the `InferencePool` resource.

In nokube mode with K8s pods you can use DNS discovery pointing at a headless
service, or run a small HTTP registry sidecar that the EPP polls.

### Docker containers

Run the EPP as a container alongside the vLLM containers (e.g. via Docker Compose).
Use file discovery with `watchFile: true` and have each vLLM container append its
endpoint to the shared backends file on startup:

```bash
# in each vLLM container entrypoint
cat >> /shared/backends.yaml << EOF
  - name: vllm-${HOSTNAME}
    address: "${MY_IP}"
    port: "8000"
EOF
vllm serve ...
```

### Bare-metal processes

Start each vLLM process and write its address to the backends file before launching.
The EPP reloads the file automatically when `watchFile: true`. A simple registration
script is sufficient:

```bash
# register.sh -- run once per vLLM process
cat >> /etc/epp/backends.yaml << EOF
  - name: vllm-$(hostname)-${PORT}
    address: "$(hostname -I | awk '{print $1}')"
    port: "${PORT}"
EOF
```

</details>

---

## Appendix B: integration with Slurm

<details>
<summary>Show</summary>

The nokube EPP can run as a standalone inference router on a Slurm cluster with no
Kubernetes dependency. The EPP and Envoy run on a head or service node; vLLM runs as
Slurm job steps on GPU compute nodes. Clients send HTTP requests to Envoy, which
consults the EPP via ext-proc to pick the best vLLM instance, then routes the request.

```
Clients
  HTTP request
    Envoy proxy (:80)  -- ext-proc callback -->  nokube EPP (:9002)
                                                   reads /shared/backends.yaml
                                                   picks best vLLM (KV cache, load)
    Envoy routes to selected vLLM instance

Slurm compute nodes
  vLLM job step (:8000)  -- appends address to /shared/backends.yaml on startup
  vLLM job step (:8000)
  ...
```

The EPP uses KV-cache utilization, queue depth, and load metrics scraped from each vLLM
instance's `/metrics` endpoint to make routing decisions. No extra tooling is needed
beyond vLLM, Envoy, and the EPP binary.

### Prerequisites

- A shared filesystem visible to both the head node and compute nodes (NFS, Lustre, GPFS)
- Envoy proxy binary or container on the head/service node
- `epp` binary on the head/service node

### Step 1: initialize the backends file

Create the backends file on the shared filesystem before the job starts:

```bash
mkdir -p /shared/llm-d
echo "backends: []" > /shared/llm-d/backends.yaml
```

### Step 2: EPP config

```yaml
# /shared/llm-d/epp-config.yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig

plugins:
  - type: file-backend-discovery
    name: slurm-discovery
    parameters:
      path: /shared/llm-d/backends.yaml
      watchFile: true
  - type: queue-depth-scorer
  - type: kv-cache-utilization-scorer
  - type: prefix-cache-scorer

schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: queue-depth-scorer
        weight: 2.0
      - pluginRef: kv-cache-utilization-scorer
        weight: 2.0
      - pluginRef: prefix-cache-scorer
        weight: 3.0

dataLayer:
  sources:
    - pluginRef: metrics-data-source
      extractors:
        - pluginRef: metrics-extractor

backendDiscovery:
  pluginRef: slurm-discovery
```

### Step 3: start the EPP on the head node

```bash
./epp \
  --config-file /shared/llm-d/epp-config.yaml \
  --pool-name slurm-pool \
  --secure-serving=false \
  --grpc-port 9002 \
  --grpc-health-port 9003 \
  --metrics-port 9090 &
```

### Step 4: Envoy proxy

Envoy sits in front of the EPP and handles the actual TCP routing to vLLM. It calls
the EPP via the ext-proc filter on every request; the EPP responds with the address of
the selected backend and Envoy forwards the request there.

See [Appendix C: Envoy configuration for Slurm](#appendix-c-envoy-configuration-for-slurm)
for a complete static Envoy config and a script to generate it from the backends file.

### Step 5: vLLM job step registration

Each vLLM task registers itself to the shared backends file before starting to serve:

```bash
#!/bin/bash
# vllm_task.sh -- run as a Slurm step on each GPU node
MY_IP=$(hostname -I | awk '{print $1}')
PORT=8000
NAME="vllm-$(hostname)-${SLURM_PROCID}"

python3 - << EOF
import yaml, fcntl

path = "/shared/llm-d/backends.yaml"
entry = {
    "name": "${NAME}",
    "address": "${MY_IP}",
    "port": "${PORT}",
    "labels": {"model": "${MODEL_NAME}"}
}
with open(path, "r+") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    data = yaml.safe_load(f) or {"backends": []}
    data["backends"].append(entry)
    f.seek(0)
    yaml.dump(data, f)
    f.truncate()
EOF

vllm serve "${MODEL_NAME}" --host 0.0.0.0 --port ${PORT}
```

The EPP picks up each new backend within seconds because `watchFile: true` triggers a
reload on every write to the backends file.

### Step 6: Slurm job script

```bash
#!/bin/bash
#SBATCH --job-name=llm-d-inference
#SBATCH --nodes=4
#SBATCH --gpus-per-node=8
#SBATCH --ntasks-per-node=1

# Start vLLM on each compute node
srun bash vllm_task.sh &

# Wait for all backends to register and become ready
sleep 30

# Clients can now send requests to Envoy on the head node
echo "Inference stack ready at http://$(hostname):80"
wait
```

### Cleanup on job exit

When the Slurm job ends, vLLM processes are killed by the scheduler. The backends file
will still contain the stale entries. Clear it in the job epilogue so the next job starts
clean:

```bash
# /etc/slurm/epilog.d/cleanup-epp.sh
echo "backends: []" > /shared/llm-d/backends.yaml
```

Alternatively, the EPP's metrics staleness threshold handles this automatically: once a
vLLM instance stops responding to `/metrics` scrapes, the EPP stops routing to it.

</details>

---

## Appendix C: Envoy configuration for Slurm

<details>
<summary>Show</summary>

### How routing works

The EPP sets the `x-gateway-destination-endpoint: <ip>:<port>` request header in its
ext-proc response. Envoy's `cluster_header` route action reads this header and selects
the cluster whose name matches. Each vLLM backend is registered as a cluster named
`<ip>:<port>`. The EPP's selection is therefore enforced exactly — Envoy routes to the
specific backend the EPP chose.

```
client request
  -> Envoy listener (:80)
       -> ext-proc filter calls EPP (:9002)
            EPP sets x-gateway-destination-endpoint: 10.0.0.3:8000
       -> route: cluster_header = x-gateway-destination-endpoint
            selects cluster named "10.0.0.3:8000"
  -> vLLM on 10.0.0.3:8000
```

### Static config template

This template has one cluster per vLLM backend. Replace the cluster entries with real
node IPs, or use the generation script below.

```yaml
# envoy.yaml
static_resources:

  listeners:
  - name: ingress
    address:
      socket_address: { address: 0.0.0.0, port_value: 80 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress
          http_filters:

          # Step 1: call the EPP for every request
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              grpc_service:
                envoy_grpc:
                  cluster_name: epp
              processing_mode:
                request_header_mode: SEND
                response_header_mode: SKIP
                request_body_mode: NONE
                response_body_mode: NONE
              # EPP sets x-gateway-destination-endpoint; clear cache so the new
              # cluster_header value is picked up on this request.
              mutation_rules:
                allow_all_routing: true

          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

          route_config:
            virtual_hosts:
            - name: vllm
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route:
                  # Step 2: route to the cluster named by the EPP-set header
                  cluster_header: x-gateway-destination-endpoint
                  timeout: 600s

  clusters:

  # EPP cluster (gRPC, HTTP/2)
  - name: epp
    type: STATIC
    connect_timeout: 1s
    http2_protocol_options: {}
    load_assignment:
      cluster_name: epp
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: 127.0.0.1, port_value: 9002 }

  # One cluster per vLLM backend — name must match x-gateway-destination-endpoint value
  - name: "10.0.0.1:8000"      # replace with real node IP
    type: STATIC
    connect_timeout: 5s
    load_assignment:
      cluster_name: "10.0.0.1:8000"
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: 10.0.0.1, port_value: 8000 }

  - name: "10.0.0.2:8000"      # replace with real node IP
    type: STATIC
    connect_timeout: 5s
    load_assignment:
      cluster_name: "10.0.0.2:8000"
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: 10.0.0.2, port_value: 8000 }

admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 9901 }
```

### Generating the config from the backends file

Since the cluster list must match the registered vLLM backends, generate `envoy.yaml`
from the same `backends.yaml` that the EPP reads. Run this after all vLLM job steps have
registered:

```python
#!/usr/bin/env python3
# generate_envoy_config.py
import yaml, sys

BACKENDS_FILE = "/shared/llm-d/backends.yaml"
OUTPUT_FILE   = "/shared/llm-d/envoy.yaml"
EPP_PORT      = 9002
LISTEN_PORT   = 80

with open(BACKENDS_FILE) as f:
    data = yaml.safe_load(f)

backends = data.get("backends", [])
if not backends:
    print("No backends registered yet", file=sys.stderr)
    sys.exit(1)

clusters = [
    {
        "name": "epp",
        "type": "STATIC",
        "connect_timeout": "1s",
        "http2_protocol_options": {},
        "load_assignment": {
            "cluster_name": "epp",
            "endpoints": [{"lb_endpoints": [{"endpoint": {"address": {
                "socket_address": {"address": "127.0.0.1", "port_value": EPP_PORT}
            }}}]}],
        },
    }
]

for b in backends:
    name = f"{b['address']}:{b['port']}"
    clusters.append({
        "name": name,
        "type": "STATIC",
        "connect_timeout": "5s",
        "load_assignment": {
            "cluster_name": name,
            "endpoints": [{"lb_endpoints": [{"endpoint": {"address": {
                "socket_address": {"address": b["address"], "port_value": int(b["port"])}
            }}}]}],
        },
    })

config = {
    "static_resources": {
        "listeners": [{
            "name": "ingress",
            "address": {"socket_address": {"address": "0.0.0.0", "port_value": LISTEN_PORT}},
            "filter_chains": [{"filters": [{"name": "envoy.filters.network.http_connection_manager", "typed_config": {
                "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                "stat_prefix": "ingress",
                "http_filters": [
                    {"name": "envoy.filters.http.ext_proc", "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor",
                        "grpc_service": {"envoy_grpc": {"cluster_name": "epp"}},
                        "processing_mode": {"request_header_mode": "SEND", "response_header_mode": "SKIP",
                                            "request_body_mode": "NONE", "response_body_mode": "NONE"},
                        "mutation_rules": {"allow_all_routing": True},
                    }},
                    {"name": "envoy.filters.http.router", "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
                    }},
                ],
                "route_config": {"virtual_hosts": [{"name": "vllm", "domains": ["*"], "routes": [
                    {"match": {"prefix": "/"}, "route": {
                        "cluster_header": "x-gateway-destination-endpoint",
                        "timeout": "600s",
                    }}
                ]}]},
            }}]}],
        }],
        "clusters": clusters,
    },
    "admin": {"address": {"socket_address": {"address": "127.0.0.1", "port_value": 9901}}},
}

with open(OUTPUT_FILE, "w") as f:
    yaml.dump(config, f, default_flow_style=False)

print(f"Wrote {len(backends)} backend clusters to {OUTPUT_FILE}")
```

In the Slurm job script, call this after waiting for backends to register, then start Envoy:

```bash
# wait for backends, then generate config and start Envoy
sleep 30
python3 generate_envoy_config.py
envoy -c /shared/llm-d/envoy.yaml &
```

If vLLM backends are added or removed during a job, regenerate the config and send
Envoy a hot-restart signal (`kill -USR2 <envoy_pid>`) to pick up the new cluster list
without dropping in-flight requests.

</details>
