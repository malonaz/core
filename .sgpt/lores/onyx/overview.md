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
| `MainManifest` | one binary: the servers it runs and the services on them | `main.go` — flags, connections, services, servers, health, shutdown |

`tools/onyx` is the generator (`--mode service|main`), `malonaz/onyx/v1`
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
