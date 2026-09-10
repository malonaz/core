---
title: AIP codegen (protoc-gen-core) — overview
description: How a resource proto becomes a model, a Postgres store and a gRPC server — the three plugins, the annotations that drive them, resource-tree semantics (patterns, singletons, silent resources, descendants), the error and lifecycle contracts every generated RPC shares, and how to customize.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# AIP codegen

`tools/protoc-gen-core` turns an AIP resource proto into three Go packages;
Google's AIPs (131–136, 154, 155, 164) are the spec and are not repeated
here — this documents what our implementation decides on top of them.

| Plugin | Emits | Driven by |
|---|---|---|
| `model` | `gengo/.../model`: struct per resource with `db` tags, `FromPb`/`ToPb`, `Parse{Resource}Name`, `Err{Resource}{AlreadyExists,NotExist,AlreadyDeleted,NotDeleted,HasChildren,ETagChanged}` | `google.api.resource` + `malonaz.codegen.model.v1.model_opts` (see `lores/style/protobuf`) |
| `postgres` | `gengo/.../store`: one `Store` per proto package, `BatchInsert/Update/Get/BatchGet/List/Delete/Undelete/Search` per resource (singletons: `Update/Get/BatchGet/List` only), raw SQL | same, plus `field_opts` joins |
| `rpc` | `gengo/.../{service}/rpc`: `{Service}Server` embedding one `{service}_{Resource}Server` per resource, each over a `{service}_{Resource}Store` interface | `malonaz.codegen.aip.v1.standard_method` on each RPC, `pagination`/`ordering`/`filtering`/`update` on requests, `malonaz.codegen.nats.v1.event` on resources |

A method is generated iff it carries `standard_method.resource` **and** is
named `{Create,Get,Update,Delete,Undelete}{Singular}` / `{BatchCreate,List,BatchGet,Search}{Plural}`;
anything else is a codegen error. Wiring (`manifest.yaml` → `service.tmpl.go`)
is onyx (`malonaz/onyx/v1` manifests, `tools/onyx`, `build_defs/codegen/onyx`); the reference implementation for every
feature is `malonaz/test/library` + `go/test/library/library_service/sat`.

## Resource tree semantics

The `resource` package resolves every pattern in a **proto package** (the
registry is per package — cross-package parent/child links do not exist):

- **Parent** = the longest registered prefix of a pattern. Stripping the final
  `collection/{id}` pair is tried before the second-to-last variable, so a
  collection may sit under a singleton (`…/{contact}/activity/events/{event}`).
- **Singleton** = pattern ending on a literal (`…/authors/{author}/profile`).
  No `Create`/`Delete` RPCs; the row is inserted with its parent and deleted
  with it. Its `delete_time`-ness must match the parent's (codegen error).
- **Multi-pattern** resources (e.g. `Note` under Organization | Author | Shelf)
  live in one table: shared identifier columns are `NOT NULL`, pattern-specific
  ones nullable, and unset identifiers match `IS NULL`. They cannot declare
  joins, and cannot have descendants.
- **Silent** resources are declared with a file-level `resource_definition`
  (or a message without `model_opts`), e.g. the library `Organization`. They
  have no table: no store, no RPCs, and cascades traverse but skip them.
- **Identifier columns** are `{variable}_id`; `model_opts.id_column_name`
  renames only the resource's own (last) identifier. Every descendant table
  carries all ancestor identifier columns, which is what makes
  parent-scoped queries and cascades plain `WHERE` clauses.

## Contracts shared by every generated RPC

- **Names**: wildcards are rejected (`InvalidArgument`); names are parsed by
  the model's `Parse{Resource}Name`. `-` (or empty) as a parent identifier in
  `List` means "across all parents".
- **Soft delete** is opted in by a nullable `delete_time` field. Tombstones are
  returned by `Get`/`BatchGet`, hidden from `List` unless `show_deleted`,
  rejected by `Update` (`NotFound`), and restored only by `Undelete` — which
  every soft-deletable resource with a `Delete` **must** declare (AIP-164).
- **Etag** is opted in by an `etag` field: computed with `aip.ComputeETag`
  over the whole resource on every write; a mismatched client etag on
  `Update`/`Delete` is `Aborted`.
- **Model errors → codes**: `NotExist`→`NotFound`, `AlreadyExists`/
  `NotDeleted`→`AlreadyExists`, `ETagChanged`→`Aborted`, `HasChildren`→
  `FailedPrecondition`; anything else goes through `status.FromError`.
- **Store ↔ RPC contract**: the RPC layer only ever calls the generated
  `{service}_{Resource}Store` interface, so a hand-written store can replace
  the Postgres one.
- **Generated SQL is concatenated, never formatted**: every read is
  `"SELECT " + QualifyColumns(cols, table) + joinSelectExprs + " FROM t " +
  joinClause + " " + where…`; columns are always table-qualified (matching the
  filter transpiler's `WithFQN`). Only the `INSERT INTO t %s VALUES %s`
  template consumed by `postgres.BatchInsertQuery` is a format string, and the
  RETURNING list is appended after it. A `%` in a join filter (`title =
  "Draft*"` → `LIKE 'Draft%'`) or a WHERE clause is therefore just a `%`
  (`sat/query_join_test.go: TestQueryJoin_WildcardFilter`).
- **Query joins are total**: `ORDER BY <order_by>, <child id>` so `LIMIT 1`
  picks the same row in the lateral join (Get/List) and in each RETURNING
  subquery (Create/Update), or chained fields could name different rows.
- **Non-nullable columns**: a `repeated`/`bytes`/map field has no proto3
  presence, so an omitted one is stored empty (`{}`), never `NULL`. A
  message-typed field (incl. `Duration`/`Timestamp`) has presence, so a
  non-nullable, non-`OUTPUT_ONLY` one **must** declare
  `(buf.validate.field).required = true` — codegen fails otherwise — so the
  omission is `InvalidArgument` at the boundary rather than an Internal error
  from `FromPb`. `nullable` shapes the column; buf.validate states the
  client's obligation; the two must agree.
- **Events**: with `malonaz.codegen.nats.v1.event` on the resource,
  `Create`/`BatchCreate`/`Update`/`Delete`/`Undelete` publish typed JetStream events after the write
  (subject = `resource_segments… . subject . subject_fields…`, optional CEL
  gate). Cascaded descendants and singleton children publish nothing.

## Customizing

- **Override an RPC**: the hand-written `Service` embeds the generated
  `*rpc.{Service}Server`; define the method on `Service` to shadow it and
  delegate to `s.{Service}Server.{Method}` for the generic part.
- **Hooks**: `middleware.RegisterHookHandler[T]` runs typed handlers on any
  request/response message (or nested message) — see
  `go/grpc/middleware/hooks.go` and `runtime.go` in the library service.
- **Extra store methods**: add them on a hand-written type that embeds the
  generated `store.Store`.

Per-RPC lores: `lores/aip/codegen/{create,batch-create,get,batch-get,list,update,delete,undelete}`;
search is `lores/aip/search`.
