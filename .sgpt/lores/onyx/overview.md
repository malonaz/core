---
title: Onyx — services and binaries from manifests
description: How a Go service and a Go binary are assembled from YAML manifests typed as malonaz/onyx/v1 protos — ServiceManifest and MainManifest, the dependency and server kinds, what tools/onyx generates for each, the build rules, the flag namespaces the generated code exposes, and the add-a-service / add-a-binary checklists.
labels:
    lang: go, protobuf
    repo: core
    topic: onyx, codegen
---

# Onyx

Onyx is how a Go service becomes a running process without hand-written
wiring. Two manifests, both YAML typed as `malonaz/onyx/v1` protos:

| Manifest | Describes | Generates |
|---|---|---|
| `ServiceManifest` | one Go service: its package, generated servers, constructor arguments | `service.tmpl.go` — the `Service` struct, `New(opts, deps...)`, `Start` |
| `MainManifest` | one binary: the servers it runs and the services on them | `main.go` — flags, connections, services, servers, health, shutdown; and its Kubernetes base (`--mode k8s`) |

`tools/onyx` is the generator (`--mode service|main|k8s`), `malonaz/onyx/v1`
the schema, `build_defs/codegen/onyx` the rules. The reference binary is
`cmd/library-service` over `go/test/library/library_service`; what the
generated main does at runtime is `lores/onyx/binary`.

## ServiceManifest

```yaml
name: library-service            # kebab-case; flag namespace + service account
target: //go/test/library/library_service
include_runtime: false           # true: generate empty Opts/runtime/start()
export_service_fields: false     # true: exported struct fields, for embedding
codegens:
  - rpc:                         # a protoc-gen-core rpc server the Service embeds
      name: library-service
      store: library             # the postgres_db_client that backs it; omit for a resource-less service
      target: //gengo/test/library/library_service/rpc
      nats: true                 # needs a `nats` dependency
      longrunning: true          # runs Operations on the scheduler; needs a grpc_client on it
dependencies:                    # constructor arguments, in this order
  - nats: {}
  - postgres_db_client:
      name: library
      target: //gengo/test/library/store
      # database: library        # defaults to name; `{database}-postgres` flags
  - grpc_client:
      service: scheduler-service
      proto: //malonaz/scheduler/scheduler_service/v1
```

Dependency kinds (`Dependency.kind`, a `oneof` — one key per list entry):

| Kind | Constructor argument | Binary provides |
|---|---|---|
| `grpc_client {service, proto, name?, operations?}` | `{name}Client pb.{Service}Client` (name defaults to service; set it when one binary hosts two same-named services on different protos); with `operations: true`, also `{name}OperationsClient longrunningpb.OperationsClient` on the same connection — the Operations of the server serving it, for waiting on the service's long-running methods (which it must have) | a connection, in-process or remote (see below) |
| `postgres_db_client {name, database?, target}` | `{name}PostgresStore *store.Store` | `target.New(psqlClient)` over `{database}` |
| `postgres_client {name, database?}` | `{name}PostgresClient *postgres.Client` | the raw client of `{database}` |
| `nats {}` | `natsClient *nats.Client` | the one NATS client |
| `service {name, target}` | `{name}Service *target.Service` | a plain component built from `opts.{Name}Service`, shared by the binary |

The package must define `Opts`, `runtime`, `newRuntime(*Opts)` and
`(*Service).start(ctx) (func(), error)` unless `include_runtime: true`
generates empty ones. A `longrunning` codegen without a `scheduler-service`
client is a generation error.

## MainManifest

```yaml
name: onikisu
setup: true                      # the package defines setup(ctx) error, run first
servers:
  - name: twilio-service         # flag namespace `twilio-service-grpc`
    grpc:
      interceptors: [INTERCEPTOR_SESSION_LOCAL_CONTEXT_INJECTOR, ...]
      descriptor_set: descriptor_set.bin   # embedded, for reflection
      services:
        - service: twilio-service
          proto: //twilio/twilio_service/v1
          manifest: //twilio/twilio_service:manifest
          gateway: true          # also serve over grpc-gateway
  - name: kanshi
    http:
      services:
        - manifest: //app/kanshi/kanshi_service:manifest   # implements RegisterRoutes
  - name: onikisu-external-grpcweb
    grpc_web_proxy: {}
  - name: intent-processor
    processor:
      manifest: //intent/intent_processor:manifest
```

Server kinds: `grpc`, `http`, `grpc_web_proxy`, `processor` (a service with
no listener — started and health-checked). Interceptors are the
`Interceptor` enum, outermost first. **Order servers however you like: the
generator starts them in dependency order** (`lores/onyx/binary`).

Listener fields. The generated main bakes them into its flag defaults
(`flags.ParseWithDefaults`), so the binary binds the same thing locally and
in Kubernetes; env and flags still override them:

| Field | Meaning |
|---|---|
| `port` | TCP port; defaults to grpc 9090, http 8080, grpc_web_proxy 8443 |
| `grpc.gateway_port` | the gateway's port; defaults to 8080; needs a service with `gateway` |
| `internal` | reached only within the pod: a grpc server binds `/tmp/{name}.socket` with TLS off; nothing is exposed |
| `public` | each of its listeners gets an Ingress |
| `hosts` | hostnames the load balancer's 443 routes to the server's listener; needs `public` |
| `grpc.gateway_hosts` | the same for the gateway |

A processor takes none of them, and `internal` excludes `public`. What each
server binds is resolved once, by `manifest.Listeners`.

## Kubernetes

`onyx_k8s(name, manifest, image?)` writes the binary's base next to its
manifest: `serviceaccount.yaml`, `deployment.yaml`, `service.yaml`,
`networkpolicy.yaml`, `pdb.yaml` (`maxUnavailable: 1`), `ingress.yaml` (only
with a public server) and the `kustomization.yaml` listing them. The objects
are `k8s.io/api` types (Kubernetes 1.37), written through their JSON form with the `status` and unset fields
(`creationTimestamp: null`, `{}`) dropped, keys sorted. Nothing in it is
environment specific:

- The Deployment sets no env: the ports come from the binary's defaults.
  Probes hit the `health` container port (`:4040/liveness`, `/readiness`);
  Prometheus (`:13434`) is announced by `prometheus.io/*` annotations and a
  `metrics` port.
- The pod is hardened: it runs as the images' `appuser` (uid 10001,
  `runAsNonRoot`), with `RuntimeDefault` seccomp, no privilege escalation, all
  capabilities dropped, a read-only root filesystem with an `emptyDir` on
  `/tmp` (sockets, temp files) and on each of the manifest's `writable_dirs`,
  and no service account token (IRSA and Pod Identity project their own).
- The manifest's `shutdown_seconds` is the termination grace period, and
  `startup_seconds` (default 300) the startup probe's budget, e.g. for
  setup's migrations.
- Listener ports in the ephemeral range (32768-60999) are reserved with the
  `net.ipv4.ip_local_reserved_ports` sysctl, so an outbound connection made
  before a server binds cannot take its port.
- The NetworkPolicy allows ingress from anywhere, only to the health,
  metrics and exposed listener ports. An overlay narrows `from` (the
  namespace and ingress controller for listeners, monitoring for metrics),
  adds a namespace default deny and restricts egress (DNS, databases, AWS).
- TLS between pods is `go/certs`: clients verify the server against
  `CA_FILE`, so a server cert names `localhost` (in-binary dials) and
  `{name}.{namespace}.svc`. Caller identity is
  `INTERCEPTOR_INTERNAL_SERVICE_AUTHENTICATION`, not mTLS.
- The Service has a port per non-internal listener, named by its flag
  namespace (`{server}-grpc`, `{server}-grpc-gateway`, ...); a grpc server's
  own port sets `appProtocol: grpc`.
- Each public listener gets an Ingress of that name with no host, its
  Service port by number (an Ingress names a port only up to 15
  characters) as the `defaultBackend`: the listener's default action, so any
  Ingress's host rule on that port ranks first. A listener with `hosts` also
  gets `{listener}-hosts`, a rule per host on 443 with the same target group
  settings. It carries the AWS Load Balancer Controller's per-listener
  annotations, which other controllers ignore: `listen-ports` on HTTPS at the
  listener's port, and the target group's health check. A grpc listener is
  `backend-protocol-version: GRPC`, checked on its own port at
  `/grpc.health.v1.Health/Check` for code 0; another listener of an onyx
  binary at `/readiness` on 4040, of an external binary at its `http_health_path`.
- `resources` on the MainManifest (`requests`/`limits` of `cpu` and
  `memory`, Kubernetes quantities) become the container's resources.
- `image` is a `docker_image` rule whose fqn is templated in; without it the
  image is the binary's name.

A binary onyx does not generate (e.g. a Python server) gets the same base
from an `ExternalManifest` (`malonaz/onyx/v1/external.proto`) through
`onyx_k8s_external(name, manifest = "external.yaml", image?)`: its `name`,
`command`, `listeners` (`name`, `port`, `grpc`, `public`,
`http_health_path`), and the MainManifest's `resources`, `shutdown_seconds`,
`startup_seconds` and `writable_dirs`. With no onyx health or Prometheus
servers, it is probed over grpc.health.v1 on its first gRPC listener, or else
over HTTP on its first listener with a health path; CEL requires one. Its
Service ports and Ingresses are `{binary}-{listener}`.

An environment's kustomize overlay adds the rest: image tag, replicas, env,
the IRSA annotation on the ServiceAccount, the Ingress class, and the
load balancer's settings (certificates, scheme, WAF, a public port other than
the listener's), namespace. Two listeners on one port (e.g. a gateway and an http
server both defaulting to 8080) fail k8s generation.

## Build rules

```python
subinclude("//build_defs/codegen/onyx:build_defs")   # ///core//... from other repos

onyx_manifest(
    name = "manifest",
    deps = ["//gengo/test/library/store", "//malonaz/scheduler/scheduler_service/v1"],
)
onyx_service_src(name = "service.tmpl.go", manifest = ":manifest")   # or onyx_main_src(name = "main.go", ...)
```

`onyx_manifest` rewrites every label in `deps` that the manifest names to
`//label(files…)`, which is how the generator finds the manifests, protos
and packages behind a label. **Every label the manifest mentions must be
in `deps`**, or it reaches the generator unresolved and fails there. A main
manifest lists the service manifests and the proto libraries; a service
manifest lists its stores, rpc packages and client protos.

Generated files carry `labels = ["codegen", "go"]`; add
`copy_generated_code::` to mirror them into the source tree like onikisu
does.

## Flags the generated main exposes

Every namespace is also an env prefix (`twilio-service-grpc` →
`TWILIO_SERVICE_GRPC_*`):

| Namespace | Type | For |
|---|---|---|
| `logging`, `health`, `prometheus`, `certs` | core opts | always |
| `session-manager`, `{interceptor}` (`jwt-authentication`, …) | authentication opts | when any grpc server has interceptors / that interceptor |
| `{database}-postgres` | `postgres.Opts` | each database |
| `nats` | `nats.Opts` | when any service depends on nats |
| `{service}-grpc` | `grpc.ClientOpts` | **only** gRPC dependencies not served by this binary |
| `{server}-grpc`, `{server}-grpc-gateway` | `grpc.ServerOpts`, `grpc.GatewayOpts` | each grpc server (+ gateway if any service has `gateway: true`) |
| `{server}-http`, `{server}-grpcwebproxy` | `http.Opts`, `grpcwebproxy.Opts` | each http / proxy server |
| `{name}-service` | component `Opts` | each `service` dependency |
| `{service}` | service `Opts` | each service |

A gRPC server named after the one service it serves (`name: twilio-service`
serving `twilio-service`) therefore gets exactly one set of flags,
`twilio-service-grpc`, that both binds the listener and is what in-process
clients dial. Name servers after their service when they serve one.

## Checklists

**New service**: package with `Opts`/`runtime`/`start` (or
`include_runtime`) → `manifest.yaml` → `onyx_manifest` + `onyx_service_src`
in BUILD, `:service.tmpl.go` in the `go_library` srcs → add it to a binary.

**New binary**: `manifest.yaml` → `onyx_manifest` (deps: every service
manifest and proto library named) + `onyx_main_src` → `go_binary` with
`:main.go` and, when `setup: true`, a `setup(ctx) error` in the package →
deps on every package the generated file imports (the build error lists
them).

## Traps

- Unknown YAML keys are errors (strict protojson), as are `type:` style
  discriminators from the pre-onyx manifests: each dependency, codegen and
  server is a single-key map naming its kind.
- Empty kinds are written `nats: {}`, `grpc_web_proxy: {}` — YAML has no
  block form for them.
- `Dependency.service` is a plain component, not a gRPC service; gRPC
  dependencies are `grpc_client`.
- A label in a manifest that is not in the rule's `deps` is not rewritten
  and the generator fails with `non-canonical label` / `names no files`.
- Two manifests with the same `name` in one binary, a dependency cycle
  between services, or two generated identifiers colliding (e.g. a server
  named like a remote client) fail the build with the offending names.
