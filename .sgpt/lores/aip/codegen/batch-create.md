---
title: AIP codegen — BatchCreate
description: What the generated BatchCreate{Plural} does beyond AIP-233 — request shape, batch/sub-request parent agreement, atomic one-round-trip insert, request-ordered response, idempotent replay of a whole batch, validate_only, and the requests[i]-prefixed error contract.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# BatchCreate

Generated for `BatchCreate{Plural}` (`rpc/batch_create.go`,
`postgres/insert.go`). Reference: library `BatchCreateAuthors` (singleton
child), `BatchCreateNotes` (multi-pattern, mixed parents),
`BatchCreateShelves` (joins + NATS events);
SATs in `go/test/library/library_service/sat/batch_create_test.go`.

## Shape (checked at codegen time)

```proto
message BatchCreateAuthorsRequest {
  string parent = 1;                              // optional
  repeated CreateAuthorRequest requests = 2 [min_items: 1, max_items: 1000];
  bool validate_only = 3;                         // optional
}
message BatchCreateAuthorsResponse { repeated Author authors = 1; }
rpc BatchCreateAuthors(...) { post: "/v1/{parent=organizations/*}/authors:batchCreate" body: "*" }
```

- `requests` must be `repeated` of the resource's `Create{Singular}Request`
  (or, if the service has no `Create`, that message becomes the resource's
  create request and must itself carry `request_id`).
- The response field is named after the resource's plural and typed as it.
- `parent` on the batch request requires `parent` on the sub-request.
- buf.validate runs over the nested `requests`, so sub-request violations are
  reported as `requests[i].author.email_address: …` by the interceptor.
  Note: the library sub-requests mark `parent` required, so inheriting the
  batch parent is only reachable for services that relax that.

## Semantics

- **Parent agreement**: wildcards in `parent` are `InvalidArgument`. If set,
  each sub-request's `parent` is either empty (inherits it) or equal to it;
  anything else is `InvalidArgument`. Unset, sub-requests may target
  different parents (the multi-pattern case).
- Each sub-request goes through the same `prepareCreate{Resource}` as
  `Create`: ids, name, timestamps, `x-migration-request`, etag, singleton
  children. Its errors are re-raised with the same code and a
  `requests[i]: ` prefix.
- `requests[i].validate_only = true` is `InvalidArgument`; use the batch's
  own `validate_only`, which returns the resources as they would be stored
  without touching the database.
- Duplicate resource names or duplicate `request_id`s within one batch are
  `InvalidArgument` — a duplicate key inside one multi-row upsert is a
  Postgres *cardinality* violation, not a unique violation, so it is caught
  before the store.
- **Atomic, one round trip**: one multi-row `INSERT … RETURNING` (plus one
  per singleton child type) in a single transaction. Any collision rolls the
  whole batch back with `AlreadyExists`; nothing is partially created.
- **Response order = request order**: `RETURNING` order is not guaranteed,
  so the store re-keys rows by `request_id`.
- **Idempotent replay**: replaying an entire committed batch (same
  `request_id`s, with or without client ids) returns the original resources.
  A batch mixing replayed and new rows also succeeds; a row whose identifier
  exists under a foreign `request_id` fails the batch.
- **Events**: one created event per resource, published after commit, in
  response order.
- No long-running operation: the batch completes synchronously.
