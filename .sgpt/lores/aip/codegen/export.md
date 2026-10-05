---
title: AIP codegen — Export
description: The AIP-153 Export{Plural} contract protoc-gen-core enforces and generates — request shape (parent, optional filter/show_deleted, oneof destination, required request_id), the shared malonaz.aip.v1.ExportMetadata, the generated keyset-paged reader of the resource, and one runner method per destination.
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
  bool show_deleted = 5;                           // optional, soft-deletable resources only
  oneof destination {                              // MUST, even with one variant
    option (buf.validate.oneof).required = true;
    CsvDestination csv_destination = 3;            // any number of *Destination messages, free-form
  }
  string request_id = 4 [required, uuid];          // MUST be required
}
message ExportBooksResponse {
  string csv = 1;            // free-form: whatever the destinations echo
}
```

- The export reads the resource itself; anything related is the destination's
  business.
- Wildcard parents are allowed (`organizations/x/shelves/-`), as in List.
- `standard_method.emit_event` is rejected: an export never emits events.
- The resource needs `model_opts`: the reader reads its table directly.

## What is generated

- `Run{Export}` on the service server: builds the reader, dispatches on the
  destination to the runner's
  `{Export}To{Variant}(ctx, request, reader *{Export}Reader) (*{Export}Response, error)`
  (`csv_destination` → `ExportBooksToCsv`), which writes the resources and
  returns the response, echoing what it wrote (e.g. the File it created).
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

A retried attempt starts over from the first page: destinations must
tolerate a rewrite (overwrite the File, keyed on `request_id`).
