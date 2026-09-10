---
title: Onyx — what the generated main does
description: The runtime model of an onyx binary — one service instance per manifest, in-process gRPC dependencies dialed through the server's own opts, servers started in dependency order behind Listen(), what each health entry checks, shutdown order, and the validations (cycles, collisions) that fail generation.
labels:
    lang: go
    repo: core
    topic: onyx, codegen, grpc
---

# The generated main

`tools/onyx --mode main` resolves a `MainManifest` into a graph
(`tools/onyx/binary/model.go`) and prints it (`generate.go`). Reading the
output of `plz build //cmd/library-service:main.go` is the fastest way to
see it; this lore is the rules behind it.

## Resolution

- **One instance per service manifest.** A manifest registered on several
  servers (onikisu's `ai-service` on the internal and external servers) is
  constructed and `Start`ed once, at the first server in start order that
  hosts it, and registered on each.
- **A gRPC dependency is in-process when a grpc server of the binary
  serves that `proto:service`.** The first such server wins. In-process
  clients get no `{service}-grpc` flags: they dial
  `opts.{Server}GRPC.ClientOpts()`, i.e. the server's own socket or
  `localhost:port`, TLS mirrored. Only dependencies nobody in the binary
  serves are remote and get `grpc.ClientOpts` flags.
- Connections are shared per endpoint string (two remote clients pointed at
  the same address share one `grpc.Connection`); same for postgres clients.
- Stores are distinct by `target`; a store declared over two databases is an
  error. Components (`service` dependencies) are distinct by `target` and
  must agree on `name`.

## Start order

Servers start in **dependency order**: a server after every server whose
services one of its own services dials in-process. Manifest order is kept
otherwise. Within a server: construct + `Start()` its not-yet-started
services, build the server, then `Listen(ctx)` **synchronously**, then
`Serve` in a goroutine.

That `Listen` is why a service's `Start()` may call an in-process
dependency: the dependency's server is already bound (connections queue in
the backlog until `Serve` accepts), not merely scheduled. Before onyx this
was a race that showed up as "connection refused" at startup.

Two things are rejected at generation time because no order satisfies them:

- **Service cycle** — `a-service -> b-service -> a-service` via in-process
  `grpc_client`s. A service dialing itself is fine and ignored.
- **Server cycle** — services acyclic but hosted so that servers depend on
  each other (`A` hosts `a→b`, `B` hosts `b'→a'`). The error suggests
  co-hosting or re-splitting.

A service's `Start()` must not call *itself* over gRPC: its own server is
bound only after `Start()` returns (deadlock, not an error).

## Health

`healthServer` (`/health`, the `health` flags) aggregates one entry per
server name. Each entry is:

| Server | Entry checks |
|---|---|
| grpc | the server's gRPC health, one service per registration: that service's postgres `Ping`s and the health of every in-process/remote gRPC dependency **except itself** |
| http | same, keyed by service name |
| processor | the service's own `HealthCheck` |
| grpc_web_proxy | the proxy's backend check |

Because cycles are rejected, dependency health checks across servers in
the same binary cannot deadlock at `NOT_SERVING`.

## Shutdown

Stops run in **reverse start order**: gRPC servers, gateways, http servers,
proxies, then the health server; `defer`s then close services, NATS,
postgres and gRPC connections. First signal → `GracefulStop` chain
(bounded by each server's `graceful-stop-timeout`); a second signal, or a
server's `Serve` returning an error, → `Stop` chain.

## Reading the output

Identifiers are derived, so you can predict them: service `twilio-service`
→ `twilioService`, `opts.TwilioService`; its gRPC client → `twilioServiceClient`,
`twilioServiceHealthCheck`; store `twilio` → `twilioStore`,
`twilioPsqlClient`; server `onikisu-external` → `onikisuExternalGRPCServer`,
`onikisuExternalGRPCGateway`, `opts.OnikisuExternalGRPC`. Collisions
between any two of these fail generation naming both owners.

Services with a `longrunning` codegen also get
`longrunningpb.RegisterOperationsServer` on every grpc server that
registers them (AIP-151).
