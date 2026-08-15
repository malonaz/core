@summary(Protobuf style guide: AIP resource patterns, naming, codegen model options, field behaviors, buf.validate validation, comments.)

# Style Guide

### File Structure
- **Syntax**: Always `syntax = "proto3";`.
- **Package**: Use `<domain>.v1` format (e.g., `chat.v1`, `user.v1`).
- **Import Order**: Sorted alphabetically. Group by: 1) `buf/validate`, 2) same-package imports, 3) `google/` imports, 4) `malonaz/` imports.
- **Options before fields**: Resource options and model options come immediately after the message declaration, before any fields.

### Naming Conventions
- **Messages**: PascalCase nouns (e.g., `ChatMetadata`, `CallRoutingRule`).
- **Fields**: snake_case (e.g., `create_time`, `phone_number`).
- **Enums**: UPPER_SNAKE_CASE values prefixed with the enum name (e.g., `CHAT_STATE_ACTIVE` for `ChatState`).
- **Enum zero value**: Always `<ENUM_NAME>_UNSPECIFIED = 0` with comment "Used to detect an unset field."
- **Resource names**: Use `name` as field 1 with `IDENTIFIER` behavior.
- **Timestamps**: Fields 2-4 are always `create_time`, `update_time`, `delete_time` in that order.

### Resource Patterns
- **google.api.resource**: Every top-level entity must declare `type`, `pattern`, `singular`, `plural`.
- **Pattern format**: `<parent_collection>/{<parent_id>}/<collection>/{<id>}` (e.g., `organizations/{organization}/users/{user}/chats/{chat}`).
- **Cross-references**: Use `google.api.resource_reference` with the full resource type (e.g., `user.onikisu.com/Contact`).
- **External resources**: Declare via `google.api.resource_definition` at file level when referencing resources from other packages.

### Model Options
- **`malonaz.codegen.model.v1.model_opts`**: Applied to messages that map to database tables.
- **`schema_name`**: Set the Postgres schema housing the table (e.g., `{schema_name: "project"}`).
- **`table_name`**: Only set when the table name differs from the snake_case message name.
- **Nullable fields**: Use `(malonaz.codegen.model.v1.field_opts).nullable = true` for optional fields (especially `delete_time`).
- **JSON storage**: Use `(malonaz.codegen.model.v1.field_opts).as_json_bytes = true` for complex nested messages stored as JSON in the database.
- **Joins**: Use `(malonaz.codegen.model.v1.field_opts).join = {parent: "...", field: "..."}` for OUTPUT_ONLY fields projected from a parent resource.

### Codegen Options (resource messages)
- **`malonaz.codegen.aip.v1.uuid_namespace`**: A fixed UUID per resource message — deterministic resource IDs.
- **`malonaz.codegen.nats.v1.event`**: Declares the NATS event stream: `stream`, `resource_segments`, and `created`/`updated`/`deleted` subjects (optionally with `subject_fields` like `["state"]`).
- Option order after the message declaration: `google.api.resource`, then nats event, then model opts, then uuid namespace.

### Field Behaviors
- **`IDENTIFIER`**: Always on `name` (field 1).
- **`OUTPUT_ONLY`**: Always on `create_time`, `update_time`, `delete_time`.
- **`REQUIRED`**: Use `(buf.validate.field).required = true` instead of `google.api.field_behavior` for validation.
- **`IMMUTABLE`**: On set-once references (e.g., a parent-like cross-reference), combined with `(buf.validate.field).required = true`.

### Validation
- **Import**: `buf/validate/validate.proto`.
- **Enums**: Use `(buf.validate.field).enum = { defined_only: true, not_in: [0] }` to reject unspecified values where required.
- **OUTPUT_ONLY enums**: `.enum.defined_only = true` alone (no `not_in`), since the server sets them.
- **Strings**: Use `(buf.validate.field).string.email`, `.pattern`, etc.
- **Oneof**: Use `(buf.validate.oneof).required = true` when exactly one variant must be set.
- **CEL**: Use `(buf.validate.message).cel` for cross-field validation rules.
- **Ignore zero**: Use `(buf.validate.field).ignore = IGNORE_IF_ZERO_VALUE` when validation should only apply to set fields.

### Standard Resource Fields
- **`etag`**: Server-computed checksum, sendable in update/delete requests for optimistic concurrency.
- **`labels`**: `map<string, string>`, stored `as_json_bytes` + `nullable`, validated with `(buf.validate.field).map` (max_pairs, key/value patterns).
- **`external_id`**: Plain string linking to a legacy/external system row; empty for native resources.

### Metadata Pattern
- Top-level resources have a `<Resource>Metadata` message for non-indexed data.
- Metadata is stored as JSON bytes: `(malonaz.codegen.model.v1.field_opts).as_json_bytes = true`.
- Metadata is nullable: `(malonaz.codegen.model.v1.field_opts).nullable = true`.
- OUTPUT_ONLY projections (e.g., settlement state) live inside metadata too, marked `(google.api.field_behavior) = OUTPUT_ONLY`.

### Service Methods
- **Standard methods**: Follow AIP-13x; every RPC comment cites its AIP (e.g., `// See: https://google.aip.dev/133 (Standard methods: Create).`).
- **`google.api.http`**: Every RPC declares its HTTP binding (`post`/`patch`/`get`/`delete` with resource-name path templates, `body` for Create/Update).
- **`google.api.method_signature`**: Always set (`"parent,project"`, `"project,update_mask"`, `"name"`, `"parent"`, ...).
- **`malonaz.codegen.aip.v1.standard_method`**: `.resource = "<domain>.onikisu.com/<Resource>"` on every standard method — drives codegen.
- **Section banners**: Group RPCs per resource with `// ===== <Resource> Management =====` comments.

### Enum Placement
- Enums closely tied to a single message live in the same file.
- Shared enums or enums used across messages can be at file level.

### Labels
- Each domain declares its well-known label keys in a `{domain}/v1/labels.proto` via file-level `(malonaz.codegen.aip.v1.label)` options: `key` (namespaced, e.g. `user.onikisu.com/creator-id`) + a thorough `description` documenting semantics, value format and how it relates to neighboring labels.

### Gateway Protos
- External exposure lives in a separate gateway package (`gateway.<domain>.v1`, service `<Domain>Gateway`): RPCs reuse the internal service's request messages and proxy to it via `(malonaz.codegen.gateway.v1.opts).proxy = "<internal service>.<Method>"`.
- Each RPC declares its `google.api.http` binding; read-only RPCs add `option idempotency_level = NO_SIDE_EFFECTS;`.
- RPC comments use `// @comment(<internal RPC>)` to inherit the internal method's documentation; gateway-only RPCs (and `google.api.HttpBody` responses for raw payloads) are written out in full.
- Imports needed only by codegen carry `// buf:lint:ignore IMPORT_USED (needed for gateway handler codegen).`

### Comments
- Every message, field, and enum value must have a leading comment.
- Comments are single-line, starting with the entity name for top-level types (e.g., "A chat represents...").
- Field comments describe what the field holds, not implementation details.
- Use `// Format: <pattern>` to document resource name formats in string fields.
- Comments on enum value = 0 is always `// Used to detect an unset field.`
- Cross-reference other messages/fields with doc links: `[QuoteRevision][project.v1.QuoteRevision]`.
- Resource-level comments explain ownership and design intent (what the resource owns vs references, who maintains projections).
