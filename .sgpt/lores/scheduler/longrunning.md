---
title: Long-running operations on the scheduler
description: How an AIP-151 rpc returning google.longrunning.Operation is run as a scheduler job — the annotations, the generated producer/runner split, the operation name ({job parent}/operations/{job}) and why it is not nested under the acted-on resource, the per-server Operations server onyx derives from the protos (gateways included) and its method scope, request_id idempotency, traps.
labels:
    lang: go, protobuf
    repo: core
    topic: scheduler, longrunning, aip
---

# Long-running operations

An operation **is** a scheduler job seen through its method's contract. There
is no operation table, column or ID: `go/scheduler/longrunning` projects jobs
onto `google.longrunning.Operation` and back. Reference implementation:
`ImportBooks` in `go/test/library/library_service` and its sats.

## Declaring one

```proto
rpc ImportBooks(ImportBooksRequest) returns (google.longrunning.Operation) {
  option (google.longrunning.operation_info) = {response_type: "ImportBooksResponse" metadata_type: "ImportBooksMetadata"};
  option (malonaz.codegen.scheduler.v1.method).policy = {attempt_timeout: {seconds: 30} max_attempts: 3};
}
```

- `operation_info` must set both types; the `scheduler.v1.method` policy is
  what makes the dispatcher create the method's queue.
- The request must carry a `parent` or `name` with a `resource_reference`:
  the job's parent derives from it (`longrunning.JobParentOf`). An optional
  `request_id` (UUID) makes starting idempotent — the repeat returns the same
  operation.
- The service manifest sets `longrunning: true` on the rpc codegen and needs a
  `grpc_client` on `scheduler-service`.

## What is generated

`{Method}` on the rpc server is one handler in two roles, selected by the
scheduler's `x-scheduler-job` metadata (`longrunning.IsRun`):

| Caller | Path |
|---|---|
| a client | `longrunning.Start`: `CreateJob(parent, request)` → returns the operation, not done |
| the dispatcher | `s.runner.Run{Method}(ctx, request)`, then `longrunning.Done`/`Failed` wrap the outcome as a **done** operation |

The service implements `Run{Method}(ctx, request) (*Response, error)`. Return
errors as gRPC statuses: the dispatcher unwraps the done operation and applies
the queue's retry policy to the error code as if the call had returned it.
`longrunning.ReportProgress(ctx, client, metadata)` records `metadata`; it
fails `FAILED_PRECONDITION` once the job left RUNNING (cancelled or reaped),
which is how a runner notices cancellation.

## Naming

```
organizations/{o}/jobs/{j}            ⇄  organizations/{o}/operations/{j}
organizations/{o}/users/{u}/jobs/{j}  ⇄  organizations/{o}/users/{u}/operations/{j}
jobs/{j}                              ⇄  operations/{j}
```

`longrunning.OperationName(jobName)` and its inverse are pure string
projections; the operation lives under the **job parent** (the organization or
user the acted-on resource belongs to), not under the resource itself. This
mirrors Google's APIs (`projects/*/locations/*/operations/*`, never under the
leaf) and is deliberate:

- one name shape → one HTTP binding, one authz rule, one `ListOperations`
  for "everything in flight in this organization";
- operations outlive or precede their resource (imports, deletes); a leaf
  parent would dangle;
- the parent is the authorization scope, which the scheduler already enforces
  on jobs.

The acted-on resource stays visible in the job's request payload.

## The Operations server

`google.longrunning.Operations` is a **server** concern: one registration per
grpc server, which onyx derives from the protos of the services it hosts
(`lores/onyx/binary`) — no manifest flag. Every rpc returning
`google.longrunning.Operation` contributes its job method: the method itself,
or, for a gateway method carrying `(malonaz.codegen.gateway.v1.opts).proxy`,
the internal method it proxies to (that is what the job row carries). The
server is `longrunning.NewServer(schedulerClient, methods)` and is **scoped by
method**: only jobs whose `method` is in the list are visible, so services
sharing a scheduler never see each other's operations — a job of another
method under the same parent is `NOT_FOUND` through it. Every job of a scoped
method is an operation, however it was enqueued.

The scheduler client is one a hosted service declares (`grpc_client` on the
`malonaz.scheduler.scheduler_service.v1.SchedulerService` proto, whatever its
`name`); a server exposing operations with no such client, or two different
ones, fails generation. Calls forward the caller's context, so the scheduler
authorizes `GetJob`/`ListJobs` as the caller — exposing Operations on an
external server is just proxying the LRO method in a gateway proto.

- `GetOperation`/`WaitOperation`/`CancelOperation`/`DeleteOperation` map to
  the job RPCs; `WaitOperation`'s timeout is capped by
  `--scheduler-service.wait-job-max-timeout`.
- `ListOperations.name` is a job parent (organization, user, or empty for
  system jobs); the only filter supported is `done` / `NOT done`.

## Traps

- A gateway that proxies an LRO needs a `grpc_client` on the scheduler
  service in its manifest even though its handler never calls it: the server's
  Operations server reads jobs through it.
- A runner handing back an unfinished operation is a bug: the dispatcher fails
  the job `FAILED_PRECONDITION` without retry.
- Stock `operations.proto` binds `GET /v1/{name=operations/**}`; a REST
  surface for parent-nested names needs a `google.api.http` override for
  `Operations.GetOperation` etc. (as every Google API does). gRPC and grpc-web
  need nothing.
