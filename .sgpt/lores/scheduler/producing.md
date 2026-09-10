---
title: Producing scheduler jobs
description: How a producer enqueues work on the scheduler — scheduler.CreateJob(ctx, client, parent, message, options…), the parent the caller names, request_id (idempotent create, v7 by default) versus unique_key (coalescing), schedule/expire/priority/labels, and what the payload type decides.
labels:
    lang: go
    repo: core
    topic: scheduler
---

# Producing jobs

`go/scheduler` is the producer side. One call, one error:

```go
_, err := scheduler.CreateJob(ctx, s.schedulerServiceClient, organization.GetName(),
    &pb.ProvisionOrganizationRequest{Name: organization.GetName()},
    scheduler.WithUniqueKey("provision/"+organization.GetName()),
    scheduler.WithScheduleTime(at),
)
if err != nil {
    return status.FromError(err, "creating job").Err()
}
```

- **`parent`** is where the job lives and what authorizes reads of it: the
  user (`organizations/{o}/users/{u}`) or organization the work belongs to,
  `""` for a system job. The caller names it — there is no derivation helper;
  a `Call`'s job goes under `callRn.UserResourceName().String()`. For a job of
  a long-running method use what `Start` would derive (user, else
  organization), so the operation lists with the ones started through the rpc.
- **`message`** is the request of the method that will run; its type URL
  selects the queue, hence the method, endpoint and retry policy. A message
  that cannot be packed (invalid UTF-8 in a string field) fails the call with
  `INVALID_ARGUMENT` — the same error, not a second one to handle.

## Options

| Option | Field | Semantics |
|---|---|---|
| `WithRequestID(id)` | `request_id` | **Idempotent create**: the same id returns the job it first created, forever (even cancelled). `CreateJob` fills a fresh v7 when unset, so retrying one request never creates a twin. Deliberate keys derive from the resource's proto UUID namespace: `uuid.NewV5(ns, "<method>/<seed>")`. |
| `WithUniqueKey(key)` | `unique_key` | **Coalescing**: at most one PENDING and one RUNNING job per key; a create while one is PENDING returns it, while one is RUNNING queues a single trailing run. Keys are global — namespace them (`"analyze_call/"+name`). |
| `WithScheduleTime(t)` | `schedule_time` | earliest run |
| `WithExpireTime(t)` | `expire_time` | must have started by then, else `DEADLINE_EXCEEDED` |
| `WithPriority(p)` | `priority` | higher runs first among due jobs |
| `WithLabels(m)` | `labels` | free-form, filterable |

`request_id` answers "did *this request* already happen"; `unique_key` answers
"is *this work* already queued". A retrying webhook wants the former; a
"re-run once whatever happens" trigger wants the latter.

## On the processor side

`scheduler.JobFromIncomingContext(ctx)` returns the job name the dispatcher
sent under `x-scheduler-job`; `longrunning.IsRun` is built on it.
