---
title: AIP codegen — Import
description: The AIP-153 Import{Plural} contract protoc-gen-core enforces and generates — request shape (parent, oneof source with a mandatory InlineSource, required request_id), names response, the shared malonaz.aip.v1.ImportMetadata, the generated sink (stamping, import-source/import-time labels, batch insert with per-row fallback, progress), one runner method per custom source, what an import never does (events, updates), and why the x-migration-request header is on its way out.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen, import
---

# Import

Generated for `Import{Plural}` (`rpc/import.go`): a long-running standard
method (AIP-151 + AIP-153) on a resource the service owns. Reference
implementation: library `ImportBooks` (`malonaz/test/library/library_service/v1/book.proto`,
`go/test/library/library_service/import_books.go`, sats in
`sat/longrunning_test.go`) — copy it.

Export is not generated yet; when it is, the same shapes apply with
`oneof destination`, `ExportMetadata` and a response echoing the resolved
destination (server-created `File` when the destination is a platform File).

## The contract (codegen refuses anything else)

```proto
rpc ImportBooks(ImportBooksRequest) returns (google.longrunning.Operation) {
  option (google.api.http) = { post: "/v1/{parent=organizations/*/shelves/*}/books:import" body: "*" };
  option (google.longrunning.operation_info) = {
    response_type: "ImportBooksResponse"
    metadata_type: "malonaz.aip.v1.ImportMetadata"     // shared, never per-method
  };
  option (malonaz.codegen.aip.v1.standard_method).resource = "library.test.malonaz.com/Book";
  option (malonaz.codegen.scheduler.v1.method).policy = { ... };   // it is an LRO
}

message ImportBooksRequest {
  string parent = 1 [required, (google.api.resource_reference).child_type = "…/Book"];
  oneof source {                                   // MUST, even with one variant
    option (buf.validate.oneof).required = true;
    InlineSource inline_source = 2;                // MUST exist, exactly this shape
    TitlesSource titles_source = 3;                // any number of *Source messages, free-form
  }
  string request_id = 4 [required, uuid];         // MUST be required
}
message InlineSource { repeated Book books = 1; }   // exactly one field: repeated {Resource} {plural}
message ImportBooksResponse {
  repeated string names = 1 [(google.api.resource_reference).type = "…/Book"];   // exactly this
}
```

- Data-level configuration common to every source goes at the top level of
  the request; source-specific configuration inside its `*Source` message.
- `standard_method.emit_event` is rejected: **an import never emits events**
  (a backfill must not fan out).
- The resource needs a `Create`/`BatchCreate` in the same service: the import
  reuses `prepareCreate{Resource}` (`lores/aip/codegen/create`).
- Singletons cannot be imported; singleton children are created alongside,
  as `Create` does.
- A bridge service that does not own the resource (e.g. a Google calendar
  bridge importing `Calendar`) declares no `standard_method` on its import:
  it gets the LRO plumbing only, hand-writes its runner and feeds the owner's
  `Import{Plural}(InlineSource)`.

## What is generated

- `Run{Import}` on the service server: rejects a wildcard parent, dispatches
  on the source — `InlineSource` is imported by generated code in batches of
  500; every other variant calls the runner's
  `Import{Plural}From{Variant}(ctx, request, sink *Import{Plural}Sink) error`
  (`titles_source` → `ImportBooksFromTitles`). Returns
  `{names}` in import order.
- `Import{Plural}Sink`, the only way resources reach the store:
  - `Import(ctx, items) (stored, error)` — stamps, inserts, reports. The
    resources come back as stored, so a source can post-process them (start
    a watch, …). An item that fails is a **partial failure**: recorded in the
    metadata, left out of the result, no error. The error returned means
    the operation was **cancelled** — stop.
  - `Fail(ctx, err)` — records an item the source could not turn into a
    resource at all.
  - `SetTotal(ctx, n)` — when the source knows its size upfront.
- The runner interface lists the source methods instead of `Run{Import}`; a
  service whose imports are inline-only implements nothing.

## Stamping (per item, in `prepare`)

- `name` set → its last segment becomes `{resource}_id`; the rebuilt name
  must equal the given one, else `InvalidArgument "… is not under parent …"`
  (AIP-153: never land under another parent). No `{resource}_id` on the
  Create request → a named item is rejected.
- `create_time`/`update_time` **kept when set**, defaulted to now / to
  `create_time` otherwise (unlike `Create`, which always overwrites them).
- Labels, only when absent: `aip.malonaz.com/import-source` = the variant
  name minus `_source`, kebab (`inline`, `titles`, `google-connection`);
  `aip.malonaz.com/import-time` = the run's UTC date `YYYY-MM-DD` (label
  values allow no colons). Constants in `go/aip/labels.go`. A bridge feeding
  the owner's inline import presets `import-source` to carry the real origin.
- Per-item `request_id` = `uuidv5(request.request_id, name | index)`, so a
  retried attempt finds what an earlier one inserted (BatchInsert replay) —
  sources must therefore be **deterministic in order** or name their items.
- Everything else (etag, identifiers, singleton children) is `prepareCreate`.

## Insert and partial failures

One `BatchInsert{Plural}` per `Import` call; the batch is atomic, so when it
fails the sink retries **one row at a time** and records only the rows at
fault (`AlreadyExists` for an existing name — an import is insert-only, it
never updates). Progress (`longrunning.ImportProgress`, `go/scheduler/longrunning/progress.go`)
is reported after every batch and every failure into the shared
`ImportMetadata { success_count, failure_count, total_count, errors[] }`;
`errors` are `google.rpc.Status`, per AIP-153. Only a `FailedPrecondition`
from the scheduler (job no longer running) surfaces; a lost report is not a
lost import.

Counts are **per attempt**: a retry that replays already-imported rows
counts them as successes again.

## Migrating off `x-migration-request`

The header on `Create` (keep the client's `create_time`) is the legacy way
to backfill. Inline imports supersede it: explicit in the API, auth-gated per
method, batched, resumable, labelled. New backfills use `Import{Plural}`;
the header goes once the remaining callers have moved.
