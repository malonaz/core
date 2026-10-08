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

The first signal, or a server's `Serve` failing, runs the graceful chain:

1. the health server shuts down;
2. **processors stop, all together** (`lifecycle.CloseAll`), ahead of every
   server whatever their declaration order: a server's graceful stop can wait
   on long-lived streams, and a processor's handlers still need the servers.
   A processor's stop is its own drain (the scheduler dispatcher stops
   claiming, gives in-flight jobs `drain-timeout`, releases the rest);
3. servers in **reverse start order**: gRPC servers, gateways, http servers,
   proxies, each bounded by its `graceful-stop-timeout`.

`defer`s then close the other services, NATS, postgres and gRPC connections.
A second signal forces the `Stop` chain.

The whole chain must fit the orchestrator's stop timeout (ECS `stopTimeout`),
so closes that each wait on a drain must not run one by one: a service closing
many NATS processors (each waits out its fetch, ~1s) uses `lifecycle.CloseAll`
too.

Binaries list their deps by hand: when the generated main starts importing a
new core package, every binary whose main uses it needs the dep, or it fails
to compile.

## Reading the output

Identifiers are derived, so you can predict them: service `twilio-service`
→ `twilioService`, `opts.TwilioService`; its gRPC client → `twilioServiceClient`,
`twilioServiceHealthCheck`; store `twilio` → `twilioStore`,
`twilioPsqlClient`; server `onikisu-external` → `onikisuExternalGRPCServer`,
`onikisuExternalGRPCGateway`, `opts.OnikisuExternalGRPC`. Collisions
between any two of these fail generation naming both owners.

A grpc server whose registered protos have methods returning
`google.longrunning.Operation` also gets one
`longrunningpb.RegisterOperationsServer` (AIP-151), scoped to those methods —
a gateway method's `proxy` target stands in for it — reading jobs through the
binary's scheduler `grpc_client` (the one the long-running service declares);
none, or two different ones, fails generation (`lores/scheduler/longrunning`).
