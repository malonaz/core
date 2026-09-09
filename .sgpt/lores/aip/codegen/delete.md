---
title: AIP codegen — Delete
description: What the generated Delete{Resource} does beyond AIP-135 — soft vs hard, the children guard and `force` cascade (which descendants gate, which are tombstoned vs removed, silent resources), allow_missing quirks, and error precedence.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# Delete

Generated for `Delete{Singular}` (`rpc/delete.go`, `postgres/delete.go`,
descendant resolution in `schema/descendants.go`). SATs:
`go/test/library/library_service/sat/{delete,cascade_delete}_test.go`.

## Soft vs hard

| | Soft (`delete_time` field) | Hard |
|---|---|---|
| Store call | `SoftDelete{R}` → `UPDATE … SET delete_time = COALESCE(delete_time, now)` | `Delete{R}` → `DELETE` |
| Returns | the tombstoned resource | `Empty` |
| Second delete | `NotFound` ("already deleted") | `NotFound` |
| `allow_missing` | swallows *already deleted* and returns the tombstone; a name that **never existed is still `NotFound`** | swallows both |
| `etag` mismatch | `Aborted` (etag is recomputed over the tombstone) | `Aborted` |

## Children: guard and cascade

A resource's **descendants** are every persisted resource beneath it in the
same proto package, walked deepest-first. Two flags decide their fate:

- **Gating** — a collection resource reached from the parent through
  singletons only. Its live rows (`delete_time IS NULL` if it has the
  column, any row otherwise) block the delete with `FailedPrecondition`
  unless `force` is set. Singletons never gate (they share the parent's
  lifecycle); nothing beneath a gating resource gates either.
- **SoftDelete** — tombstone instead of `DELETE`, iff the parent *and every
  resource on the path* are soft-deletable. Rows must never outlive a
  hard-deleted ancestor, so `Shelf(soft) → Book(hard) → Bookmark(soft)`
  hard-deletes bookmarks.

Silent resources (no message / no `model_opts`, e.g. a `resource_definition`
child) have no rows: they neither gate nor get deleted, but their persisted
descendants still do.

Codegen consequences, all in one transaction:

- If the resource has any gating descendant, its `Delete…Request` **must
  declare `bool force`** — otherwise codegen fails — and the store method
  gains a `force bool` parameter.
- `if !force`: one `SELECT EXISTS(...) OR EXISTS(...)` over the gating
  descendants → `Err{R}HasChildren` → `FailedPrecondition`.
- Then every descendant is deleted, unconditionally. After a passing guard
  this only touches tombstones (a hard-deleted parent purges its
  soft-deleted children rather than tripping a FK) — so the cascade is
  idempotent and never depends on `ON DELETE CASCADE`.
- Cascaded rows keep their `etag`; only `delete_time` is stamped.
- Cascaded descendants publish **no** NATS events and run no hooks.

Multi-pattern resources cannot have descendants (codegen error).

## Error precedence

Soft: `NotFound` / `Aborted` (parent row) before `FailedPrecondition`
(children). Hard: `FailedPrecondition` before `NotFound` / `Aborted`,
because children must be removed before the parent row can be. Both orders
are stable and tested; do not "fix" one to match the other without moving
the guard.
