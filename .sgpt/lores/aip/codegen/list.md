---
title: AIP codegen — List
description: What the generated List{Plural} does beyond AIP-132 — the three request-message options that drive it, offset page tokens, parent wildcards, show_deleted, and how filters/order_by become SQL.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# List

Generated for `List{Plural}` (`rpc/list.go`, `postgres/list.go`). Client-side
filter syntax is in `lores/aip/querying`; this is the server side.

## Request options (all on the `List…Request` message)

```proto
option (malonaz.codegen.aip.v1.pagination) = {default_page_size: 100};
option (malonaz.codegen.aip.v1.ordering)   = {paths: ["*"], default: "create_time desc"};
option (malonaz.codegen.aip.v1.filtering)  = {paths: ["*"]};
```

- `paths` accept `*` and `prefix.*` wildcards and are validated against the
  resource at startup (`MustNew…Parser` panics on a bad path).
- `ordering.default` is mandatory and applied when `order_by` is empty;
  `ordering.paths` is the allow-list for client `order_by`.
- Filter and order_by are transpiled to SQL by `go/aip/transpiler/postgres`
  with table-qualified columns, so they compose with joins. JSONB
  (`as_json_bytes`) fields and labels are addressable by path.

## Pagination

Page tokens are einride offset tokens: `OFFSET n LIMIT page_size+1`, the
extra row only decides whether `next_page_token` is emitted. The token
embeds a checksum of the request, so changing `filter`/`order_by`/
`page_size` mid-iteration is `InvalidArgument`. Offsets are not stable
under concurrent inserts; use `aip.Paginate` (see `lores/style/go`) rather
than hand-rolled loops.

## Parent

- Single-pattern: `parent` is `Sscan`-ed; an identifier of `-` (or empty)
  drops that `WHERE` term, so `organizations/-/…` lists across parents.
- Multi-pattern: `parent` is matched against the distinct parent patterns;
  identifiers of unmatched patterns are simply not filtered on, so listing
  under an Author never returns Shelf notes.

## Soft delete

Resources with `delete_time` get `bool show_deleted` on the request;
default hides tombstones (`delete_time IS NULL`). There is no "deleted only"
mode — filter on `delete_time:*` with `show_deleted: true`.

## Response shaping

Read masks are not part of codegen: the `x-read-mask` gRPC metadata
(`middleware.WithReadMask`) prunes any response, List included.
