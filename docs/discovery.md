# Discovery Plugin Architecture

## Overview

The EPP discovers inference endpoints through a **DiscoveryPlugin** -- a pluggable abstraction
that populates and maintains the endpoint datastore independently of the underlying
infrastructure. By default the EPP uses Kubernetes CRD reconcilers to discover endpoints.
When a `DiscoveryPlugin` is configured, all Kubernetes reconcilers are bypassed and the
plugin becomes the sole source of truth for endpoint lifecycle.

This enables the EPP to run without a Kubernetes cluster, which is valuable for RL training
and inference workloads on non-Kubernetes infrastructure such as **Slurm** and **Ray**
clusters.

---

## Core Interfaces

### `DiscoveryPlugin`

```go
type DiscoveryPlugin interface {
    Start(ctx context.Context, notifier Notifier) error
}
```

`Start` is the plugin's entry point. It must:
1. Enumerate all currently known endpoints and call `notifier.Upsert` for each.
2. Continue watching for changes until `ctx` is cancelled.
3. Block until `ctx` is cancelled or a fatal error occurs.

### `Notifier`

```go
type Notifier interface {
    Upsert(endpoint *EndpointMetadata)
    Delete(id types.NamespacedName)
}
```

The `Notifier` is the callback interface through which the plugin drives the datastore.

**Ordering contract:** the datastore processes `Upsert` and `Delete` calls in the order
they are received. Plugin implementations MUST preserve event order -- do not buffer or
dispatch calls concurrently in a way that could reorder them. For example, an `Upsert`
followed by a `Delete` for the same endpoint must arrive in that order, or the endpoint
will be incorrectly left in the datastore.

---

## Plugin Registration

A `DiscoveryPlugin` implementation must also implement `fwkplugin.Plugin` so it can be
registered in the plugin registry and referenced by name from the EPP config.

```go
fwkplugin.Register(myDiscovery.PluginType, myDiscovery.Factory)
```

---

## Configuration

To enable a discovery plugin, add a `discovery` section to `EndpointPickerConfig`
referencing the plugin by name:

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

When a `discovery` field is present and the referenced plugin is of type `file-discovery`,
the EPP starts in **no-kube mode**: the Kubernetes controller manager and all CRD/pod
reconcilers are not started. The EPP runs as a standalone process, requiring no cluster
connectivity.

---

## Built-in Plugins

### `file-discovery`

Reads a YAML (or JSON) file listing inference endpoints. Optionally watches the file for
changes at runtime using `fsnotify`.

**Package:** `pkg/epp/framework/plugins/discovery/file`

**Plugin type:** `file-discovery`

#### Parameters

| Parameter   | Type   | Required | Default | Description |
|-------------|--------|----------|---------|-------------|
| `path`      | string | yes      | --      | Path to the endpoints file |
| `watchFile` | bool   | no       | `false` | If true, reload the file when it changes on disk |

#### Endpoints file format

The file is YAML (JSON is also accepted). Top-level key is `endpoints`, a list of
endpoint entries.

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

When `watchFile: true`, the plugin watches the file using `fsnotify`. On any `Write` or
`Create` event:
- All endpoints present in the new file are upserted (add or update).
- Endpoints absent from the new file but present in the previous load are deleted.
- Events are always delivered in file order: upserts first, then deletes.

When `watchFile: false` (the default), the file is loaded once at startup and the plugin
blocks until the context is cancelled. Use this for static deployments where the endpoint
list is fixed at launch time.

---

## Running llm-d without Kubernetes (file-discovery mode)

This section describes how to run a complete llm-d inference stack on bare metal or VMs,
with no Kubernetes cluster required. The stack consists of:

- One or more **vLLM** inference servers (decode-only or prefill/decode pairs)
- The **pd-sidecar** proxy on each decode node (only needed for P/D disaggregation)
- The **EPP** with `file-discovery`
- **Envoy** as the ingress gateway

```
Client
  --> Envoy (port 8081)
        --[ext_proc]--> EPP (port 9002)
                          picks target endpoint, sets x-gateway-destination-endpoint
        --[ORIGINAL_DST]--> vLLM or pd-sidecar (address from header)
```

### Prerequisites

- EPP binary or container image (`ghcr.io/llm-d/llm-d-inference-scheduler`)
- Envoy container image (`envoyproxy/envoy:distroless-v1.33.2` or later)
- vLLM install or container image (`vllm/vllm-openai`)
- pd-sidecar binary or container image (`ghcr.io/llm-d/llm-d-routing-sidecar`) -- P/D only

---

## Appendix A -- EPP configuration

### EPP config file (`epp-config.yaml`)

Minimal config for file-discovery with random scheduling:

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

  - name: metrics-source
    type: metrics-data-source

  - name: core-metrics-extractor
    type: core-metrics-extractor

discovery:
  pluginRef: file-discovery

schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: random-picker

dataLayer:
  sources:
    - pluginRef: metrics-source
      extractors:
        - pluginRef: core-metrics-extractor
```

### EPP startup flags

```bash
epp \
  --config-file /etc/epp/epp-config.yaml \
  --grpc-port 9002 \
  --grpc-health-port 9003 \
  --metrics-port 9090 \
  --secure-serving=false
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config-file` | -- | Path to `EndpointPickerConfig` YAML |
| `--grpc-port` | 9002 | Port Envoy connects to via ext_proc |
| `--grpc-health-port` | 9003 | gRPC health probe port |
| `--metrics-port` | 9090 | Prometheus metrics port |
| `--secure-serving` | true | Set false to disable TLS on the ext_proc port |
| `--pool-name` | `epp` | Logical pool name (informational in no-kube mode) |
| `--pool-namespace` | `default` | Logical pool namespace (informational in no-kube mode) |

Both `--pool-name` and `--pool-namespace` can be omitted in no-kube mode; the defaults
are `epp` and `default` respectively.

### Endpoints file (`endpoints.yaml`)

```yaml
endpoints:
  - name: decode-0
    address: 192.168.1.10
    port: "8001"
    labels:
      model: Qwen/Qwen2-0.5B

  - name: decode-1
    address: 192.168.1.11
    port: "8001"
    labels:
      model: Qwen/Qwen2-0.5B
```

---

## Appendix B -- Envoy configuration

Envoy acts as the ingress gateway. It calls the EPP via ext_proc on every request to
determine the target backend, then forwards to that backend using the
`x-gateway-destination-endpoint` header.

The EPP sets `x-gateway-destination-endpoint: <ip>:<port>` in the request headers.
Envoy's `original_destination_cluster` reads this header and opens a connection directly
to that address. Envoy never needs to know the list of backends.

### `envoy.yaml`

```yaml
static_resources:
  listeners:
    - name: inference
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 8081
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: inference
                route_config:
                  virtual_hosts:
                    - name: inference
                      domains: ["*"]
                      routes:
                        - match:
                            prefix: "/"
                          route:
                            cluster: original_destination_cluster
                            timeout: 86400s
                            idle_timeout: 86400s
                            upgrade_configs:
                              - upgrade_type: websocket
                http_filters:
                  - name: envoy.filters.http.ext_proc
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
                      grpc_service:
                        envoy_grpc:
                          cluster_name: ext_proc
                          authority: epp:9002
                        timeout: 10s
                      processing_mode:
                        request_header_mode: SEND
                        response_header_mode: SEND
                        request_body_mode: FULL_DUPLEX_STREAMED
                        response_body_mode: FULL_DUPLEX_STREAMED
                        request_trailer_mode: SEND
                        response_trailer_mode: SEND
                      message_timeout: 1000s
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

  clusters:
    - name: original_destination_cluster
      type: ORIGINAL_DST
      connect_timeout: 1000s
      lb_policy: CLUSTER_PROVIDED
      original_dst_lb_config:
        use_http_header: true
        http_header_name: x-gateway-destination-endpoint

    - name: ext_proc
      type: STRICT_DNS
      connect_timeout: 10s
      lb_policy: LEAST_REQUEST
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            http2_protocol_options: {}
      load_assignment:
        cluster_name: ext_proc
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      address: epp       # replace with EPP hostname or IP
                      port_value: 9002

admin:
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 19000
```

### Key points

- Replace `epp` in the ext_proc cluster with the actual hostname or IP of the EPP
  process or container.
- `FULL_DUPLEX_STREAMED` body mode is required for streaming token-by-token responses.
- The ext_proc cluster must use HTTP/2 (`http2_protocol_options`); gRPC requires it.
- `original_destination_cluster` needs no backend list -- Envoy connects to whatever
  address the EPP puts in the header.

---

## Appendix C -- vLLM setup

### Decode-only (no P/D disaggregation)

Run a standard vLLM OpenAI-compatible server. The EPP scrapes metrics from
`http://<address>:<port>/metrics` to make scheduling decisions.

```bash
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2-0.5B \
  --port 8001 \
  --host 0.0.0.0
```

Set `metricsHost` in the endpoint entry if vLLM metrics are on a different port than
the inference port (they default to the same port).

### P/D disaggregation

For prefill/decode disaggregation, each node runs vLLM configured with a KV connector.

**Prefiller node:**
```bash
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2-0.5B \
  --port 8001 \
  --host 0.0.0.0 \
  --kv-connector PyNcclConnector
```

**Decoder node:**
```bash
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2-0.5B \
  --port 8001 \
  --host 0.0.0.0 \
  --kv-connector PyNcclConnector
```

The pd-sidecar sits in front of the decoder and orchestrates the prefill-then-decode
handshake. Clients and Envoy talk to the sidecar (port 8000), not to vLLM directly
(port 8001).

---

## Appendix D -- pd-sidecar setup (P/D disaggregation)

The pd-sidecar is a lightweight HTTP proxy that implements the prefill/decode handshake.
It runs on the decode node alongside vLLM.

### How it works

1. Client sends `POST /v1/completions` to the sidecar.
2. Sidecar reads the `x-prefiller-url` header set by the EPP and forwards the prefill
   phase to that prefiller using the configured KV connector.
3. KV cache blocks are transferred from prefiller to decoder via the connector.
4. Sidecar forwards the decode phase to the local vLLM instance.
5. Streaming response flows back to the client.

The `x-prefiller-url` header is injected by the EPP scheduling logic, which selects
the best prefiller based on KV cache occupancy and queue depth.

### Startup flags

```bash
pd-sidecar \
  --port 8000 \
  --vllm-port 8001 \
  --kv-connector nixlv2 \
  --secure-proxy=false
```

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 8000 | Port the sidecar listens on |
| `--vllm-port` | 8001 | Port of the local vLLM decoder |
| `--kv-connector` | nixlv2 | KV transfer protocol (see table below) |
| `--secure-proxy` | true | Set false to disable TLS on the sidecar port |
| `--enable-tls` | -- | Stages to enable TLS for: `prefiller`, `decoder` |
| `--tls-insecure-skip-verify` | -- | Stages to skip TLS verification for |
| `--enable-prefiller-sampling` | false | Randomly select from multiple prefiller hosts |
| `--data-parallel-size` | 1 | vLLM data-parallel size |

### Supported KV connectors

| Connector | Flag value | Notes |
|-----------|------------|-------|
| NIXL v2 | `nixlv2` | NVIDIA NIXL high-speed KV transfer; recommended for GPU clusters |
| Shared storage | `shared-storage` | KV transfer via shared filesystem (NFS, Lustre, etc.) |
| SGLang | `sglang` | SGLang P/D disaggregation protocol |

---

## Appendix E -- docker-compose example (decode-only)

A minimal setup with one EPP, one Envoy, and two vLLM decode nodes.

### File layout

```
.
|- docker-compose.yaml
|- envoy.yaml
|- epp-config.yaml
|- endpoints.yaml
```

### `endpoints.yaml`

```yaml
endpoints:
  - name: vllm-0
    address: vllm-0
    port: "8001"
    labels:
      model: Qwen/Qwen2-0.5B

  - name: vllm-1
    address: vllm-1
    port: "8001"
    labels:
      model: Qwen/Qwen2-0.5B
```

### `docker-compose.yaml`

```yaml
services:
  vllm-0:
    image: vllm/vllm-openai:latest
    command:
      - --model=Qwen/Qwen2-0.5B
      - --port=8001
      - --host=0.0.0.0
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  vllm-1:
    image: vllm/vllm-openai:latest
    command:
      - --model=Qwen/Qwen2-0.5B
      - --port=8001
      - --host=0.0.0.0
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  epp:
    image: ghcr.io/llm-d/llm-d-inference-scheduler:latest
    command:
      - --config-file=/etc/epp/epp-config.yaml
      - --grpc-port=9002
      - --grpc-health-port=9003
      - --metrics-port=9090
      - --secure-serving=false
    volumes:
      - ./epp-config.yaml:/etc/epp/epp-config.yaml
      - ./endpoints.yaml:/etc/epp/endpoints.yaml
    depends_on:
      - vllm-0
      - vllm-1

  envoy:
    image: envoyproxy/envoy:distroless-v1.33.2
    command: ["-c", "/etc/envoy/envoy.yaml"]
    ports:
      - "8081:8081"
    volumes:
      - ./envoy.yaml:/etc/envoy/envoy.yaml
    depends_on:
      - epp
```

### Send a request

```bash
curl http://localhost:8081/v1/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "Qwen/Qwen2-0.5B", "prompt": "Hello", "max_tokens": 50}'
```

---

## Appendix F -- docker-compose example (P/D disaggregation)

A 1-prefiller, 1-decoder setup. The sidecar shares the network namespace of the
decoder so it can reach vLLM on localhost.

### `endpoints-pd.yaml`

The EPP discovers the sidecar address (port 8000), not vLLM directly (port 8001).

```yaml
endpoints:
  - name: decoder-0
    address: vllm-decoder
    port: "8000"
    labels:
      role: decode
```

### `docker-compose.yaml`

```yaml
services:
  vllm-prefiller:
    image: vllm/vllm-openai:latest
    command:
      - --model=Qwen/Qwen2-0.5B
      - --port=8001
      - --host=0.0.0.0
      - --kv-connector=PyNcclConnector
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  vllm-decoder:
    image: vllm/vllm-openai:latest
    command:
      - --model=Qwen/Qwen2-0.5B
      - --port=8001
      - --host=0.0.0.0
      - --kv-connector=PyNcclConnector
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  sidecar:
    image: ghcr.io/llm-d/llm-d-routing-sidecar:latest
    command:
      - --port=8000
      - --vllm-port=8001
      - --kv-connector=nixlv2
      - --secure-proxy=false
    network_mode: "service:vllm-decoder"

  epp:
    image: ghcr.io/llm-d/llm-d-inference-scheduler:latest
    command:
      - --config-file=/etc/epp/epp-config.yaml
      - --grpc-port=9002
      - --grpc-health-port=9003
      - --metrics-port=9090
      - --secure-serving=false
    volumes:
      - ./epp-config.yaml:/etc/epp/epp-config.yaml
      - ./endpoints-pd.yaml:/etc/epp/endpoints-pd.yaml
    depends_on:
      - sidecar

  envoy:
    image: envoyproxy/envoy:distroless-v1.33.2
    command: ["-c", "/etc/envoy/envoy.yaml"]
    ports:
      - "8081:8081"
    volumes:
      - ./envoy.yaml:/etc/envoy/envoy.yaml
    depends_on:
      - epp
```

### Send a P/D request

The sidecar expects the prefiller URL via the `x-prefiller-url` header. In a full
deployment this header is injected by the EPP; for manual testing:

```bash
curl http://localhost:8081/v1/completions \
  -H "Content-Type: application/json" \
  -H "x-prefiller-url: http://vllm-prefiller:8001" \
  -d '{"model": "Qwen/Qwen2-0.5B", "prompt": "Hello", "max_tokens": 50}'
```

---

## Implementing a Custom Discovery Plugin

1. Create a struct that implements both `discovery.DiscoveryPlugin` and `fwkplugin.Plugin`.
2. Write a factory function with signature
   `func(name string, parameters json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error)`.
3. Register the factory: `fwkplugin.Register(MyPluginType, MyFactory)`.
4. Reference it in `EndpointPickerConfig` via the `discovery.pluginRef` field.

The `file-discovery` plugin at
`pkg/epp/framework/plugins/discovery/file/file_discovery.go` is the reference
implementation.
