---
title: AIP codegen — Export
description: The AIP-153 Export{Plural} contract protoc-gen-core enforces and generates — request shape (parent, required oneof destination of {x}_destination variants, optional filter/show_deleted, required request_id), the response's mirroring oneof result of {x}_result variants, the shared malonaz.aip.v1.ExportMetadata, the generated keyset-paged reader of the resource, and one runner method per destination that writes it out and returns its result.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen, export
---

# Export

Generated for `Export{Plural}` (`rpc/export.go`): a long-running standard
method (AIP-151 + AIP-153) on a resource the service owns. Reference
implementations: library `ExportBooks` and `ExportShelves`, in
`malonaz/test/library/library_service/v1/{book,shelf}.proto`,
`go/test/library/library_service/export.go`, sats in `sat/export_test.go`.

## The contract (codegen refuses anything else)

```proto
rpc ExportBooks(ExportBooksRequest) returns (google.longrunning.Operation) {
  option (google.api.http) = { post: "/v1/{parent=organizations/*/shelves/*}/books:export" body: "*" };  // {parent} in the URI
  option (google.longrunning.operation_info) = {
    response_type: "ExportBooksResponse"
    metadata_type: "malonaz.aip.v1.ExportMetadata"     // shared, never per-method
  };
  option (malonaz.codegen.aip.v1.standard_method).resource = "library.test.malonaz.com/Book";
  option (malonaz.codegen.scheduler.v1.method).policy = { ... };   // it is an LRO
}

message ExportBooksRequest {
  option (malonaz.codegen.aip.v1.filtering) = {paths: [...]};  // MUST when `filter` exists
  string parent = 1 [required, (google.api.resource_reference).child_type = "…/Book"];
  string filter = 2;                               // optional, AIP-160, same SQL as List
  bool show_deleted = 4;                           // optional, soft-deletable resources only
  string request_id = 3 [required, uuid];          // MUST be required
  oneof destination {                              // MUST, (buf.validate.oneof).required = true
    CsvDestination csv_destination = 5;            // every variant `{x}_destination`, a *Destination message
    TitlesDestination titles_destination = 6;
  }
  // anything else is free-form, for the runner to read
}
message ExportBooksResponse {                      // nothing but the oneof
  oneof result {                                   // MUST, mirrors `destination`
    CsvResult csv_result = 1;                      // exactly one `{x}_result` per `{x}_destination`,
    TitlesResult titles_result = 2;                // a *Result message: where the resources landed
  }
}
```

- The pairing is Cloud Asset's: `OutputConfig.destination { gcs_destination }` ↔
  `OutputResult.result { gcs_result }`. Per-run stats (counts, failures) belong
  to `ExportMetadata`, so the response holds nothing outside `result`.
- A result is a message, never a bare scalar, so it can grow (a row count, a
  second File) without a breaking change; share it as you share the
  destination (`platform.v1.FileDestination` ↔ `platform.v1.FileResult`).

- Destination messages may live anywhere: share one across methods and
  services (a platform-wide `FileDestination`), as Google shares `GcsDestination`.
- No destination is generated (unlike import's `InlineSource`): resources
  returned inline are unbounded, which is what List pages.

- The export reads the resource itself; anything related is the destination's
  business.
- Wildcard parents are allowed (`organizations/x/shelves/-`), as in List.
- `standard_method.emit_event` is rejected: an export never emits events.
- The resource needs `model_opts`: the reader reads its table directly.

## What is generated

- One runner method per destination,
  `{Export}To{X}(ctx, request, reader *{Export}Reader) (*{X}Result, error)`
  (`csv_destination` → `ExportBooksToCsv` returning `*CsvResult`), dispatched
  on the request's destination by the generated handler: it writes the
  resources out and returns where they landed (e.g. the File it created); the
  handler sets it as the matching `result` variant, so a runner cannot answer
  with another destination's result. For CSV, `go/pbutil/pbcsv` derives the header and rows from
  the resource's descriptor: no hand-written columns.
- `{Export}Reader`, the only way resources leave the store:
  - `Next(ctx) ([]*{Resource}, error)` — the next page (500 rows); nil once
    exhausted. Resources are counted as successes when returned. The error
    means the operation was cancelled or the read failed: stop.
  - `Fail(ctx, err)` — a resource `Next` returned that the destination could
    not write: moves it from successes to failures (`google.rpc.Status`).
  - `SetTotal(ctx, n)` — when the destination knows the size.

## Paging

Keyset, not offset: rows come in primary key order (the identifier columns,
in pattern order, under the database collation) and each page starts with
`(key columns) > (last row's key)`, so rows inserted or deleted mid-export
never shift a page. The keyset is AND-ed onto the parent and filter, which
the store already scopes. Nullable identifiers of multi-pattern resources are
compared as `COALESCE(col, '')`.

A retried attempt starts over from the first page: the runner must
tolerate a rewrite (overwrite the File, keyed on `request_id`).
