---
title: "Task: support JSONB (as_json_bytes) paths in the Search framework"
description: Extend the codegen Search framework so search fields can point into as_json_bytes metadata columns (e.g. "metadata.postal_address"), with library-service SAT coverage.
labels:
    lang: go, protobuf
    repo: core
    status: todo
    topic: aip, codegen, search
---

# Goal

Search fields (`malonaz.codegen.aip.v1.search` on a resource message) currently
accept only top-level string / repeated-string columns. Most rich data in this
codebase lives in message fields stored as JSONB via
`(malonaz.codegen.model.v1.field_opts).as_json_bytes` (the "metadata pattern").
Allow a search field `path` to reach into those, e.g.
`path: "metadata.postal_address"`.

Read `lores/aip/search` first — it describes the existing framework end to end.

# Design

- `path` may be dotted: first segment must resolve to a top-level message field
  with `as_json_bytes = true`; remaining segments walk the message definition.
- The terminal segment may be a string, repeated string, or a message — for a
  message, index the whole subtree: `coalesce(column #>> '{seg,...}', '')`
  fed to `to_tsvector('simple', ...)` tokenizes JSON text fine (punctuation
  and quotes are separators). JSONB extraction operators are IMMUTABLE, so
  generated columns keep working.
- **Verify the JSON key casing** the expression must use: read the model
  codegen's as_json_bytes marshaling (tools/protoc-gen-core/plugin/model) to
  determine whether keys are protojson camelCase or proto field names — do not
  guess; assert it in a SAT test that matches on a nested value.
- `split` and `snippet` apply as usual (snippets headline the same raw
  expression). Weights unchanged.
- Validation (codegen-time errors, follow the existing joined-field error in
  `tools/protoc-gen-core/schema/search.go`): dotted path whose first segment is
  not an as_json_bytes message field; unknown intermediate/terminal segments;
  non-string scalar terminals.

# Key files

- `tools/protoc-gen-core/schema/search.go` — path resolution + expression builder (the main change).
- `malonaz/codegen/aip/v1/aip.proto` — update `SearchOptions.Field.path` comment.
- `tools/protoc-gen-core/plugin/postgres/search.go`, `plugin/rpc/search.go` — should need no structural change.
- Reference implementation to extend: library `Author` (`malonaz/test/library/v1/author.proto`,
  its `AuthorMetadata` already holds `country`, `email_addresses`, `phone_numbers`).

# Deliverables

1. Codegen support + validations above.
2. Library test wiring: add a metadata path (e.g. `metadata.country`) to
   Author's search fields; new append-only migration in
   `go/test/library/migrations/library/` updating the `search_document`
   generated column (drop + re-add; copy the regenerated
   `AuthorSearchDocumentExpression`), and append the file + md5 hash to
   `migrations/manifest.yaml` (append-only — never edit applied entries).
3. SAT tests in `go/test/library/library_service/sat/search_test.go`:
   metadata match, metadata no-match, casing assertion, validation error cases
   exercised at codegen level where practical.
4. Update `lores/aip/search` (JSON paths paragraph).
5. `plz run //tools/tidy:lint` clean; `plz test //go/aip:test //go/test/library/... //tools/...` green.
