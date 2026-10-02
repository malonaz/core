---
title: AIP codegen — Export
description: The AIP-153 Export{Plural} contract protoc-gen-core enforces and generates — request shape (parent, optional filter/show_deleted, oneof destination with a mandatory empty nested InlineDestination, required request_id), the response's item field (the resource itself, or an aggregate the runner builds per page), the shared malonaz.aip.v1.ExportMetadata, the generated keyset-paged reader, and one runner method per custom destination.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen, export
---

# Export

Generated for `Export{Plural}` (`rpc/export.go`): a long-running standard
method (AIP-151 + AIP-153) on a resource the service owns. Reference
implementations: library `ExportBooks` (resource + custom destination) and
`ExportShelves` (aggregate, inline only), in
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
  bool show_deleted = 6;                           // optional, soft-deletable resources only
  message InlineDestination {}                     // MUST be nested here, and empty
  oneof destination {                              // MUST, even with one variant
    option (buf.validate.oneof).required = true;
    InlineDestination inline_destination = 3;      // MUST exist
    CsvDestination csv_destination = 4;            // any number of *Destination messages, free-form
  }
  string request_id = 5 [required, uuid];          // MUST be required
}
message ExportBooksResponse {
  repeated Book books = 1;   // MUST be first: repeated {Item} {plural}
  string csv = 2;            // anything else is free-form
}
```

- **The item** is the type of the response's first field:
  - the resource itself (`repeated Shelf shelves`): the export is entirely
    generated, the runner implements nothing for it;
  - any other message (`ExportedShelf { Shelf shelf; repeated Book books; }`):
    the runner always builds it, in
    `Aggregate{Export}(ctx, request, {plural} []*{Resource}) ([]*{Item}, error)`
    (`AggregateExportShelves`), called once per page so related resources are
    fetched in one query per page, never one per resource. It may return
    fewer items than resources; its error fails the attempt.
  - AIP-153: "the same format **must** be used for both import and export".
    `Import{Plural}` inlines the resource itself, so a service importing the
    resource must export it as the resource too: an aggregate is rejected.
    What an inline export returns, an inline import takes back.
- Wildcard parents are allowed (`organizations/x/shelves/-`), as in List.
- `standard_method.emit_event` is rejected: an export never emits events.
- The resource needs `model_opts`: the reader reads its table directly.

## What is generated

- `Run{Export}` on the service server: builds the reader, dispatches on the
  destination. `InlineDestination` answers with every item in `{plural}`;
  every other variant calls the runner's
  `{Export}To{Variant}(ctx, request, reader *{Export}Reader) (*{Export}Response, error)`
  (`csv_destination` → `ExportBooksToCsv`), which writes the items and
  returns the response, echoing what it wrote (e.g. the File it created).
- `{Export}Reader`, the only way items leave the store:
  - `Next(ctx) ([]*{Item}, error)` — the next page (500 rows), aggregated;
    nil once exhausted. Items are counted as successes when returned. The
    error means the operation was cancelled or the read failed: stop.
  - `Fail(ctx, err)` — an item `Next` returned that the destination could
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

## Inline vs File

The inline response is stored on the operation by the scheduler: fine for
small exports and tests. Real exports go to a custom destination, typically
a platform File the runner creates and names in the response.
