---
title: Full-text search (SearchX RPCs)
description: How to add an AIP-136 Search{Plural} RPC to a resource — the search codegen option, the migration contract (generated tsvector column), and query semantics.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen, search
---

# Full-text search

Postgres FTS, fully codegen-driven. Reference implementation: library `Author`
(`SearchAuthors`) — copy it.

## To make a resource searchable

1. **Resource proto**: add `(malonaz.codegen.aip.v1.search)` with `fields`
   (`path`, `weight` A–D, optional `split`: `SPLIT_EMAIL_ADDRESS` /
   `SPLIT_PHONE_NUMBER`). See `malonaz/test/library/v1/author.proto`.
   A `path` may also be dotted to reach into a message field stored as JSONB
   via `(malonaz.codegen.model.v1.field_opts).as_json_bytes`, e.g.
   `metadata.country`: the first segment must be an as_json_bytes message
   field, and the terminal segment a string, repeated string, or message
   (whole JSON subtree indexed). The expression uses `column #>> '{...}'`
   (IMMUTABLE, safe in generated columns); JSON keys are proto field names
   (`pbutil.JSONMarshal` sets `UseProtoNames`).
2. **Service proto**: add `Search{Plural}` request/response (like List but with
   `query`, no `order_by` — results are relevance-ranked) and the RPC with the
   usual `standard_method` option. See
   `malonaz/test/library/library_service/v1/author.proto` + `library_service.proto`.
3. **Migration**: declare a `search_document tsvector GENERATED ALWAYS AS (...)
   STORED` column + GIN index, copying the codegen-emitted
   `{Resource}SearchDocumentExpression` constant from the generated store.
   Array fields need the IMMUTABLE `core_array_to_string` wrapper function
   (`array_to_string` is only STABLE); `SPLIT_PHONE_NUMBER` fields need
   `core_phone_number_tokens` (both contracts are documented on the constants
   in `tools/protoc-gen-core/schema/search.go`). See
   `go/test/library/migrations/library/002_author_search.sql`.

## Key files

- Option: `malonaz/codegen/aip/v1/aip.proto` (`SearchOptions`).
- Expression builder: `tools/protoc-gen-core/schema/search.go`.
- Store/RPC codegen: `tools/protoc-gen-core/plugin/postgres/search.go`,
  `tools/protoc-gen-core/plugin/rpc/search.go`.
- Query → tsquery: `go/aip/search_query.go` (`BuildPrefixTSQuery`).
- SAT suite: `go/test/library/library_service/sat/search_test.go`.

## Snippets

Mark a search field `snippet: true` and declare
`repeated malonaz.aip.v1.SearchSnippet snippets` on the Search response —
index-aligned with the resource list. Each snippet holds `repeated
SearchSnippetMatch matches` (`path` + `match`), ordered by field weight (A
first); fragments are highlighted with `**` via `ts_headline` (raw text, not
the split variant). Dotted JSONB paths (e.g. `metadata.country`) work — the
snippet key stays dotted.

Snippets are opt-in per request: every Search request must declare
`bool include_snippets` (codegen-enforced). When false the query is
byte-identical to a snippet-less search (no `ts_headline` cost) and the
response carries no snippets.

## Index strategy

Multi-tenant tables should use a composite GIN index —
`USING gin (parent_id, search_document)` via `CREATE EXTENSION btree_gin` — so
token lookups scope to the tenant instead of scanning all orgs then
bitmap-ANDing. The library test uses a plain GIN only because the SAT
toolchain postgres lacks contrib extensions.

## Semantics & limits

- Case-insensitive; every token prefix-matched and AND-ed
  (`"john smi"` → `john:* & smi:*`); a query with no indexable token is InvalidArgument.
- `filter` (AIP-160) composes with `query`; ranking is `ts_rank` by field weight.
- No stemming ('simple' config), no mid-token substrings, no synonyms —
  semantic search would be a pgvector addition later.
- Exception: `SPLIT_PHONE_NUMBER` fields index every suffix (length >= 3) of the
  digits-only number via `core_phone_number_tokens`, so any fragment of a phone
  number matches (`860` → `8605979801`). One unmatched token still ANDs the
  whole result set to empty — that is how "no substring" failures present.
