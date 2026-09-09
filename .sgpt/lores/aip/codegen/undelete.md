---
title: AIP codegen — Undelete
description: What the generated Undelete{Resource} does beyond AIP-164 — it is mandatory for every soft-deletable resource with a Delete, restores lifecycle singletons but never collection children, the etag contract, error codes (ALREADY_EXISTS on live), and the undeleted event.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# Undelete

Generated for `Undelete{Singular}` (`rpc/undelete.go`, `postgres/undelete.go`).
SATs: `go/test/library/library_service/sat/undelete_test.go`, events in
`nats_events_test.go`.

## Mandatory

A service that declares `Delete{R}` for a soft-deletable resource (nullable
`delete_time`) **must** declare `Undelete{R}` in the same service, or codegen
fails. The converse is enforced too: `Undelete` on a hard-deletable resource
is a codegen error, as is an `Undelete` whose output is not the resource.

```proto
rpc UndeleteBook(UndeleteBookRequest) returns (Book) {
  option (google.api.http) = {post: "/v1/{name=shelves/*/books/*}:undelete" body: "*"};
  option (google.api.method_signature) = "name";
  option (malonaz.codegen.aip.v1.standard_method).resource = "library.example.com/Book";
}
message UndeleteBookRequest {
  string name = 1 [(buf.validate.field).required = true, (google.api.field_behavior) = REQUIRED,
                   (google.api.resource_reference).type = "library.example.com/Book"];
  string etag = 2;  // required iff the resource has an etag
}
```

The request must declare `name`, and `etag` when the resource has one (same
implicit contract as Delete). Nothing else — AIP-164 forbids other fields.

## Behaviour

| | |
|---|---|
| Store call | `Undelete{R}(ctx, ids…[, etag, newEtag])` → `UPDATE … SET delete_time = NULL[, etag] WHERE ids AND delete_time IS NOT NULL` |
| Returns | the restored resource, `delete_time` unset |
| Live resource | `AlreadyExists` (AIP-164), row untouched |
| Never existed | `NotFound` |
| `etag` mismatch | `Aborted` — checked *after* liveness, so a stale etag on a live row is still `AlreadyExists` |
| `update_time` | untouched: undelete is not an edit |
| Etag | recomputed over the restored resource (`aip.ComputeETag` with `delete_time` cleared), so it moves on undelete |

The zero-row case is disambiguated by the resource's `probe{R}`
(`SELECT delete_time IS NULL[, etag]`), the same probe Update and Delete use;
see `lores/aip/codegen/delete` for the state-before-etag precedence.

## Descendants: lifecycle singletons only

Undelete restores exactly the descendants Delete tombstones *unconditionally*:
singletons reached through singletons only (`schema.Descendant.Lifecycle`,
e.g. `Author → AuthorProfile`). They have no Delete of their own, so their
tombstone is always the parent's; they are cleared with a plain
`UPDATE … SET delete_time = NULL WHERE ids` in the same transaction.

Collection descendants are **never** restored, even when a `force` Delete
tombstoned them. Delete only takes them under an explicit opt-in and AIP-164
gives Undelete no such flag, so an unconditional cascade would be a bigger
surprise than a missing one — and it is not reversible, whereas children stay
individually recoverable (`List(show_deleted)` → `Undelete`).

Codegen does not police the parent's state either: undeleting a Note under a
tombstoned Author succeeds. Guard that in the service if it matters.

## Events

`malonaz.codegen.nats.v1.event.undeleted` publishes a
`RESOURCE_EVENT_TYPE_UNDELETED` `ResourceEvent` after the write (CEL variable:
the resource). Failed undeletes publish nothing.

## Overriding

Shadow `Undelete{R}` on the hand-written `Service` as for any generated RPC;
`pbreflection` knows `StandardMethodTypeUndelete`, so CLI/JSON schema tooling
treats it like Get/Delete (name required, no resource body).
