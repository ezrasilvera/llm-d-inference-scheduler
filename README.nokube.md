# llm-d Inference Scheduler: nokube Mode

## Overview

The inference scheduler (EPP) normally runs inside a Kubernetes cluster. It watches
`InferencePool` and `Pod` resources via the K8s API server to discover inference backends
and uses `controller-runtime` reconcilers for lifecycle management.

**nokube mode** lets the EPP run on any machine (bare metal, a VM, a developer laptop)
without a Kubernetes API server. Backend discovery is handled by a pluggable
`BackendDiscovery` plugin instead of watching K8s pods.

Key differences between modes:

| Capability | K8s mode | nokube mode |
|---|---|---|
| Backend discovery | Live pod watch via K8s API (`PodReconciler`) | `BackendDiscovery` plugin (file, HTTP, gRPC, DNS) |
| Pool configuration | Reconciled live from `InferencePool` CRD | CLI flags at startup (`--pool-name`, `--pool-namespace`) |
| `InferenceObjective` (request priority) | Reconciled live from CRD | **Static:** loaded once from `staticConfig` at startup |
| `InferenceModelRewrite` (model aliasing) | Reconciled live from CRD | **Static:** loaded once from `staticConfig` at startup |
| Lifecycle manager | `ctrl.Manager` | `errgroup` |
| High availability / leader election | Supported (K8s leases) | Not supported |

Scheduling algorithms, flow control, metrics polling from inference engines, Prometheus
metrics, TLS, and health checking all work identically in both modes.

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

The binary is compiled with K8s Go packages but does **not contact a K8s API server** at
runtime when running in nokube mode.

- `ctrl.GetConfig()` and `ctrl.NewManager()` are not called.
- Reconcilers (`PodReconciler`, `InferencePoolReconciler`) do not start.
- `BackendDiscovery` replaces pod watching.
- Lifecycle is managed by `errgroup` instead of `ctrl.Manager`.
- K8s packages (`k8s.io/api`, `sigs.k8s.io/controller-runtime`) are still compiled into
  the binary but only used for logging helpers, which have no API server dependency.

**Single binary, two execution paths.** Mode is selected automatically: if
`backendDiscovery` is present in the config, the nokube path runs; otherwise the standard
K8s path runs. `cmd/epp-nokube/` is a 15-line entrypoint that calls the same runner with
nokube mode pre-selected.

Code structure:
```
cmd/epp/runner/runner.go
  setupCommon()   <- shared by both paths (~185 lines)
  runK8s()        <- K8s-specific (~115 lines)
  runNokube()     <- nokube-specific (~90 lines)

cmd/epp/main.go          <- unchanged, auto-detects mode from config
cmd/epp-nokube/main.go   <- 15-line shim: runner.WithNokubeMode().Run(ctx)
```

### Level 2: full import isolation

The binary has no K8s package imports at all. It is built from a fully separate entry
point (`cmd/epp-nokube/`) whose import graph excludes `k8s.io/api/core/v1`,
`sigs.k8s.io/controller-runtime`, and the K8s client.

This requires a fully independent runner (~310 lines) that re-implements lifecycle
management, gRPC server startup, and Prometheus metrics serving. All of that logic already
exists in the K8s runner, resulting in ~185 lines of duplication.

Code size comparison:

| | Level 1 (selected) | Level 2 |
|---|---|---|
| `cmd/epp/runner/runner.go` | ~820 lines | 731 lines |
| `cmd/epp-nokube/runner/runner.go` | ~15 lines (shim) | 310 lines (full runner) |
| Total runner code | ~835 lines | 1041 lines |
| Duplicated setup block | 0 | ~185 lines |

### Comparison

| Property | Level 1 | Level 2 |
|---|---|---|
| Runs without K8s cluster at runtime | yes | yes |
| No K8s package imports in binary | no | yes |
| Binary size | ~10 MB larger | smaller |
| Supply-chain / SBOM cleanliness | K8s packages present | clean |
| Code duplication | ~0 lines | ~185 lines |
| Separate binary entrypoint | thin shim | full independent runner |
| Separate `Dockerfile.epp-nokube` | yes | yes |

Both levels produce a separate `epp-nokube` container image that works without a cluster.
Level 2 gives a cleaner binary; Level 1 gives simpler code with no duplication.

</details>

---

## Single image vs two images

<details>
<summary>Show</summary>

**Single image:** one image that runs in both K8s and nokube modes, selected by the
presence or absence of `backendDiscovery` in the config file. Simpler distribution but the
image name gives no signal about whether a cluster is required.

**Two images:** separate `epp` (K8s) and `epp-nokube` images built from the same codebase
via separate Dockerfiles. Each image does one thing and the name is self-documenting.

### Selected option: two images

1. **Naming clarity.** `epp-nokube` tells operators immediately that no cluster is
   required. A misconfigured `epp` image that cannot reach an API server fails at startup;
   `epp-nokube` never attempts the connection.
2. **Deployment scenario alignment.** nokube targets bare-metal, edge, and VM deployments
   where having K8s client packages in the binary may be undesirable even at Level 1.
3. **Future-proofing.** The two-image model leaves the door open to Level 2 hardening
   (full import removal) without changing the deployment model or image names.
4. **Security posture.** Separate images allow separate vulnerability scanning policies,
   separate pod security admission profiles, and separate SBOM attestations.

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

`BackendDiscovery` plugins call the notifier; `nokubeDatastoreNotifier` in
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

### 2. HTTP polling (`http-backend-discovery`)

Polls a REST endpoint on a configurable interval. The endpoint must return a JSON array
of backend objects. On each poll the full list is diffed against the current set: new
entries are upserted and missing entries are deleted.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `url` | string | required | URL to GET; must return a JSON array of backend objects |
| `refreshInterval` | duration (nanoseconds) | 30s | How often to poll |

**Response format:** a JSON array of the same structure as the file backend entries:

```json
[
  {"name": "vllm-0", "namespace": "default", "address": "10.0.0.1", "port": "8000"},
  {"name": "vllm-1", "address": "10.0.0.2", "port": "8000", "labels": {"model": "llama3"}}
]
```

**Config example:**

```yaml
plugins:
  - type: http-backend-discovery
    name: http-discovery
    parameters:
      url: "http://registry.inference.svc:8080/backends"
      refreshInterval: 15000000000    # 15s in nanoseconds

backendDiscovery:
  pluginRef: http-discovery
```

### 3. gRPC streaming (`grpc-backend-discovery`)

Subscribes to a gRPC server-streaming RPC that pushes `BackendEvent` messages. Reconnects
with exponential backoff (2s to 30s cap) on connection failure.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `address` | string | required | `host:port` of the gRPC discovery server |
| `insecure` | bool | `true` | Disable TLS verification |

**Wire format (`BackendEvent` JSON):**

```json
{"type": "UPSERT", "name": "vllm-0", "namespace": "default",
 "address": "10.0.0.1", "port": "8000", "labels": {"model": "llama3"}}
{"type": "DELETE", "name": "vllm-0", "namespace": "default"}
```

The server must implement the streaming RPC at
`/discovery.BackendDiscoveryService/WatchBackends`.

**Config example:**

```yaml
plugins:
  - type: grpc-backend-discovery
    name: grpc-discovery
    parameters:
      address: "discovery-server.inference.svc:50051"
      insecure: false

backendDiscovery:
  pluginRef: grpc-discovery
```

### 4. DNS (`dns-backend-discovery`)

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

</details>

---

## Static configuration for objectives and model rewrites

<details>
<summary>Show</summary>

In K8s mode, `InferenceObjective` (request priority) and `InferenceModelRewrite` (model
name aliasing) are reconciled from CRDs at runtime. In nokube mode they are loaded once
at startup from the `staticConfig` section of the config file.

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

### Changed: same feature, static instead of live

| Feature | K8s mode | nokube mode |
|---|---|---|
| Backend discovery | Live pod watch via K8s API | BackendDiscovery plugin |
| InferenceObjective / priority | Live CRD reconciliation | `staticConfig` at startup |
| InferenceModelRewrite | Live CRD reconciliation | `staticConfig` at startup |
| Pool config (selector, ports) | Updated live from `InferencePool` CRD | CLI flags at startup |

### Not available

| Feature | Reason |
|---|---|
| HA / leader election | Requires K8s leases |
| Dynamic pool reconfiguration at runtime | Pool is fixed at startup |
| `k8s-notification-source` plugins | No K8s API server; config validation fails fast |
| Controller-runtime reconciler metrics | No reconcilers running |

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

## Running the nokube image

<details>
<summary>Show</summary>

```bash
docker run --rm \
  -v /path/to/config.yaml:/etc/epp/config.yaml \
  -v /path/to/backends.yaml:/etc/epp/backends.yaml \
  -p 9002:9002 -p 9003:9003 -p 9090:9090 \
  ghcr.io/llm-d/llm-d-inference-scheduler-nokube:dev \
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

| vLLM deployment | K8s EPP | nokube EPP | Recommended discovery |
|---|---|---|---|
| Kubernetes pods | yes | yes | K8s mode: automatic via `PodReconciler`. nokube: DNS (headless service) or HTTP. |
| Docker containers | no | yes | File discovery or HTTP polling against a registration sidecar |
| Bare-metal processes | no | yes | File discovery (static or written by vLLM on startup) |
| Slurm job steps | no | yes | File discovery (job prologue writes the endpoint list) |

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
- `epp-nokube` binary on the head/service node

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
./epp-nokube \
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
