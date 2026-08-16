@alias("default")

You are working in malonaz/core: shared Go platform libraries (grpc, aip,
pbutil, postgres, ai), the Please build system, and protobuf codegen
plugins. Key areas: `build_defs/` (Please build rules and the
protoc_gen_core codegen templates), `go/` (platform libraries), and
generated outputs under `genproto/`/`gengo/` (never edit by hand).

# Lore index

This repo's durable knowledge, under `.sgpt/lores/`. Keep this list
updated whenever a lore is added, renamed, or removed.

- `lores/style/go` — Go style guide and preferred core libraries: gRPC
  errors, pbutil, field masks, AIP pagination, errgroup, resource names.
- `lores/style/protobuf` — protobuf style guide: AIP resource patterns,
  naming, codegen model options, field behaviors, buf.validate.
- `lores/aip/querying` — querying AIP-compliant APIs: AIP-160 filters,
  update/read masks, Get/Batch over List.
- `lores/aip/search` — adding an AIP-136 Search{Plural} RPC: search codegen
  option, tsvector migration contract, query semantics.
- `lores/domain/agent` — Agent/Task/Memory ontology, durable runners,
  wake-by-append model, Postgres SKIP LOCKED queue.
- `lores/domain/genui` — generative-UI protocol: proto components exposed as AI
  tools, streamed as tool calls, rendered by client surfaces.
- `lores/tasks/search-json-fields` — task: support JSONB (as_json_bytes)
  paths in the Search framework.

Read the relevant lore before working in an unfamiliar area.

# Commit messages

Format: `[{area}] - {lowercase summary}`

- `{area}` is the touched path or proto package: `go/ai`, `go/pbutil`,
  `tools/protoc-gen-core`, `malonaz/ai`, `.sgpt/lores`, `cmd/tsunade`.
- Summary is short, imperative, lowercase, no trailing period.
- Most commits are one line. Add a body only when the *why* is
  non-obvious: a short paragraph on the cause, then one on the fix.
