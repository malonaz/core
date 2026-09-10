---
title: Long-running operations on the scheduler
description: How an AIP-151 rpc returning google.longrunning.Operation is run as a scheduler job — the annotations, the generated producer/runner split, the operation name ({job parent}/operations/{job}) and why it is not nested under the acted-on resource, the per-service Operations server and its method scope, request_id idempotency, traps.
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

Onyx embeds `*longrunning.Server` in every service with a `longrunning`
codegen and registers `google.longrunning.Operations` on each grpc server
hosting it (`lores/onyx/binary`). The server is **scoped by method**:
`NewServer(client, slices.Concat(rpc.{Service}LongrunningMethods...))` only
surfaces jobs whose `method` is in the list, so services sharing a scheduler
never see each other's operations — a job of another method under the same
parent is `NOT_FOUND` through it. Every job of a scoped method is an
operation, however it was enqueued.

- `GetOperation`/`WaitOperation`/`CancelOperation`/`DeleteOperation` map to
  the job RPCs; `WaitOperation`'s timeout is capped by
  `--scheduler-service.wait-job-max-timeout`.
- `ListOperations.name` is a job parent (organization, user, or empty for
  system jobs); the only filter supported is `done` / `NOT done`.

## Traps

- `google.longrunning.Operations` is a single service name: a grpc server can
  register it once. Hosting two longrunning services on one server needs one
  aggregated Operations server, not two registrations.
- A runner handing back an unfinished operation is a bug: the dispatcher fails
  the job `FAILED_PRECONDITION` without retry.
- Stock `operations.proto` binds `GET /v1/{name=operations/**}`; a REST
  surface for parent-nested names needs a `google.api.http` override for
  `Operations.GetOperation` etc. (as every Google API does). gRPC and grpc-web
  need nothing.
