---
title: Recurring jobs with Schedule
description: How recurring work is declared on the scheduler — the Schedule resource (cron + payload template), what each tick materializes (a plain Job under the schedule's parent, its `schedule` field naming the schedule), tick/missed-tick/run-window semantics, pause/resume, the idempotent replay, and traps.
labels:
    lang: go, protobuf
    repo: core
    topic: scheduler
---

# Schedules

A `Schedule` (`malonaz/scheduler/v1/schedule.proto`) is recurrence declared
once: a five-field cron expression, a time zone and a payload template. The
`scheduler-service` tick routine (`go/scheduler/scheduler_service/tick.go`,
`--scheduler-service.tick-interval`, 1s) turns each due tick into an ordinary
`Job`, so a producer that used to re-enqueue its own successor at the end of
every run declares a schedule instead and stops thinking about it.

## Declaring one

```go
createScheduleRequest := &pb.CreateScheduleRequest{
    Parent: "",                       // system work; or the organization / user it belongs to
    Schedule: &schedulerpb.Schedule{
        Payload:   payload,           // anypb of the request to run; its type selects the queue
        Cron:      "0 3 * * *",       // minute hour day-of-month month day-of-week
        TimeZone:  "Europe/Paris",    // IANA; defaults to UTC
        RunWindow: durationpb.New(2 * time.Hour),
        Priority:  10,
        Labels:    map[string]string{"team": "analytics"},
    },
}
```

- The payload type must already have a queue (the dispatcher creates one per
  annotated method): `INVALID_ARGUMENT` otherwise, as for a bad cron or zone.
  `queue` and `method` are resolved at create and read-only after; `payload` is
  immutable — a different request is a different schedule.
- The parent is the parent of every job the schedule creates, and what
  authorizes reads of them. Name it as you would for `scheduler.CreateJob`.
- `Update` may change `labels`, `cron`, `time_zone`, `priority`, `run_window`;
  a `cron`/`time_zone` change recomputes `next_schedule_time` from now.
- `Delete` leaves the jobs already created alone.

## What a tick does

At `next_schedule_time` the routine locks the due schedules (`FOR UPDATE SKIP
LOCKED`, so replicas tick disjoint sets) and, per schedule:

| | |
|---|---|
| job | created through the service's own `CreateJob`: same routing, validation and `unique_key` handling as any job |
| `parent` | the schedule's |
| `schedule_time` | the tick |
| `expire_time` | tick + `run_window`; **without a run window, the next tick** — so on an irregular cron (`0 9 * * MON-FRI`) Friday's job may start until Monday |
| `priority`, `labels` | the schedule's |
| `schedule` | the schedule's name (OUTPUT_ONLY: only the tick sets it, never a producer) |
| `request_id` | `uuidv5(fixed namespace, name + "/" + tick RFC3339)` |
| schedule | `last_schedule_time = tick`, `last_job`, `next_schedule_time = cron.Next(now)` |

A tick reached after its run window already closed is **missed**: no job,
`missed_tick_count++`, and the schedule moves on. `next_schedule_time` is
always the first occurrence after *now*, never after the tick: downtime skips
the ticks it slept through (Cloud Scheduler semantics; there is no catch-up
option). A pass that dies between the insert and the advance replays into the
same job on the next pass thanks to the derived `request_id`. A tick whose job
cannot be created (its queue was deleted) is retried every pass until its
window closes, then counted missed; `scheduler_schedule_ticks_total{outcome}`
counts `created`/`missed`/`failed`, `scheduler_schedules_overdue` the enabled
schedules more than a tick interval late.

Nothing wakes the dispatcher: the job is claimed by its ordinary poll.

## Pause / resume

`PauseSchedule` clears `next_schedule_time`; nothing is materialized. `ResumeSchedule`
recomputes it from now — ticks that fell inside the pause are gone, not missed.
Both are idempotent and take an optional etag (`ABORTED` when stale). A `cron`
update on a paused schedule leaves `next_schedule_time` unset until the resume.

## Listing a schedule's jobs

```
ListJobs{parent: <schedule parent>, filter: schedule = "<schedule name>"}
```

## Traps

- **Update is not the generated one.** `UpdateSchedule` patches under the row
  lock (`store.TransitionSchedule`) so the recurrence and the
  `next_schedule_time` it implies land in one write; it reuses
  `aip.MustNewUpdateRequestParser` for the allow-list and `protovalidate` on
  the patched resource, so behaviour matches the generated Update otherwise.
- **Cron lives in `go/scheduler/cron`** (robfig/cron v3 parser, five fields,
  no `@descriptors`, no inline `TZ=` — the zone is `time_zone`). `Next` is
  strictly after its argument and returns a time in the schedule's zone; tests
  compute expectations with the same package. `time/tzdata` is embedded so a
  zoned schedule does not depend on the host's zoneinfo.
- **Tests do not wait a minute.** Sats create schedules on `0 0 1 1 *` and
  rewind `next_schedule_time` with a direct `UPDATE scheduler.schedule`, then
  poll: the routine picks the row up on its next pass (`--tick-interval 100ms`
  in the sat). Only `TestSchedule_MaterializesEveryMinute` waits for a real
  `* * * * *` tick.
- One tick pass handles `tickBatchSize` (100) schedules; a due burst larger
  than that drains one batch per interval.
