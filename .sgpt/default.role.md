@alias("default")

You are working in malonaz/core: shared Go platform libraries (grpc, aip,
pbutil, postgres, ai), the Please build system, and protobuf codegen
plugins. Key areas: `build_defs/` (Please build rules and the
protoc_gen_core and onyx codegen), `go/` (platform libraries), and
generated outputs under `genproto/`/`gengo/` (never edit by hand).

# Lore index

This repo's durable knowledge, under `.sgpt/lores/`. Keep this list
updated whenever a lore is added, renamed, or removed.

- `lores/style/go` — Go style guide and preferred core libraries: gRPC
  errors, pbutil, field masks, AIP pagination, errgroup, resource names,
  grpc ServerOpts/ClientOpts + Listen.
- `lores/style/protobuf` — protobuf style guide: AIP resource patterns,
  naming, codegen model options, field behaviors, buf.validate.
- `lores/aip/querying` — querying AIP-compliant APIs: AIP-160 filters,
  update/read masks, Get/Batch over List.
- `lores/aip/search` — adding an AIP-136 Search{Plural} RPC: search codegen
  option, tsvector migration contract, query semantics.
- `lores/aip/codegen/overview` — protoc-gen-core: the three plugins and their
  annotations, resource-tree semantics (singletons, multi-pattern, silent
  resources), contracts shared by every generated RPC (concatenated SQL, the
  no-rows probe, non-nullable = required for message fields), customizing.
- `lores/aip/codegen/{create,batch-create,get,batch-get,list,update,delete,undelete}`
  — per-RPC behaviour beyond the AIPs: mandatory request_id idempotency,
  atomic request-ordered BatchCreate, tombstone visibility, all-or-nothing
  BatchGet, list options and offset tokens, update allow-list and etag
  retry, the children guard and `force` cascade, mandatory Undelete for
  soft-deletable resources (restores lifecycle singletons only).
- `lores/onyx/overview` — onyx: ServiceManifest/MainManifest (malonaz/onyx/v1),
  dependency and server kinds, build rules, generated flag namespaces,
  add-a-service / add-a-binary checklists, YAML traps.
- `lores/onyx/binary` — what the generated main does: one instance per
  service, in-process gRPC deps dial the server's opts, dependency-ordered
  start behind Listen(), health entries, shutdown order, cycle/collision errors.
- `lores/scheduler/producing` — enqueuing work: `scheduler.CreateJob(ctx,
  client, parent, message, options…)`, the parent the caller names,
  `request_id` (idempotent, v7 default) vs `unique_key` (coalescing).
- `lores/scheduler/longrunning` — AIP-151 operations as scheduler jobs:
  annotations, generated producer/runner split, `{job parent}/operations/{job}`
  naming and why, the per-server Operations server onyx derives from the
  protos (gateways included), traps.
- `lores/scheduler/schedules` — recurring jobs: the Schedule resource (cron +
  payload template), what a tick materializes, run window / missed ticks /
  no catch-up, pause/resume, idempotent replay, how the sats avoid minute waits.
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
