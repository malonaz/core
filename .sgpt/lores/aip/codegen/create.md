---
title: AIP codegen — Create
description: What the generated Create{Resource} does beyond AIP-133 — id generation, the mandatory request_id and its idempotency semantics, the x-migration-request header, singleton children, validate_only, the error contract, and the single-element BatchInsert store contract it shares with BatchCreate.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# Create

Generated for `Create{Singular}` (`rpc/create.go`, `postgres/insert.go`).
`Create` is a one-element `BatchCreate` (`lores/aip/codegen/batch-create`):
both call a shared `prepareCreate{Resource}` (steps below) and then the one
store insert, `BatchInsert{Plural}(ctx, requestIDs, models[, children…])`.

## Identity

- `{resource}_id` unset → `aip.NewSystemGeneratedBase32ResourceID()`.
- Single-pattern: the parent is `Sscan`-ed against the parent pattern; the
  resource `name` is always rebuilt server-side (a client-supplied `name` is
  ignored).
- Multi-pattern: the parent is matched against each pattern's parent,
  most-specific first, and the matching pattern builds the name. Two
  patterns sharing a parent pattern is a codegen error — the parent alone
  could not pick the pattern.

## Idempotency (`request_id`)

The Create request **must** declare `string request_id` (codegen error
otherwise — `CreateXRequest must declare a request_id field`), and the table
needs a `request_id UUID NOT NULL UNIQUE` column. Both AIP-155 replay and
AIP-133 `AlreadyExists` are keyed on it, which is why there is no opt-out.

- Empty `request_id` is filled with a UUIDv7 server-side, so every create
  is stored with one.
- Same `request_id` again → the **existing** row is returned, no error, even
  if the caller changed the payload.
- Same identifier, different `request_id` → `AlreadyExists`.
- A unique violation on any *other* constraint of the table (a hand-written
  unique index the request ids cannot explain) → `AlreadyExists`.

How: the insert is `ON CONFLICT (identifiers) DO UPDATE SET id = id RETURNING
…, request_id` — a no-op upsert so the existing row comes back and its
`request_id` tells replay from collision. A replay with server-generated ids
misses the identifier conflict and trips the `request_id` unique constraint
instead; the store then re-reads by request id and returns the committed row.

## Timestamps and the migration header

`create_time` is stamped `now()` and copied to `update_time`. If the incoming
gRPC metadata carries `x-migration-request`, the request **must** supply
`create_time` (else `InvalidArgument`) and it is kept — the escape hatch for
backfills that need historical timestamps.

## Singleton children

Every persisted singleton child (`…/{author}/profile`) is built with the
parent's `create_time`/`update_time`, its own etag, and inserted in the
same transaction (`ON CONFLICT DO NOTHING`, so an idempotent replay is
harmless). Their content starts at proto defaults; populate via `Update`.

## `validate_only`

If the request has the field and it is set, the RPC returns the resource as
it *would* be stored (name, timestamps, etag) without touching the database.

## Errors

`AlreadyExists` as above; buf.validate violations are rejected by the
validation interceptor before the handler runs.
