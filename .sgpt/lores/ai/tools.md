---
title: 'ai: tools and tool calls as resources (Tool, ToolMethod, the ai processor, ToolCall)'
description: 'malonaz.ai.v1.Tool as a resource (platform tools/{t} mirrored by the ai processor, organizations/{o}/tools/{t} written by clients; kebab-case ids, the title is the name the model calls a stored tool by; inline tools keep a bare name), the typed ai-engine.malonaz.com/method annotation (ToolMethod) and the deprecated strings still stamped for shipped clients, the ai.malonaz.com/tool-set label, go/ai/ai_processor (the only reflector: converge, stale deletion, unreadable targets), ToolUse (the block value) vs ToolCall (the resource …/messages/{m}/toolCalls/{n}, written with its message in one transaction and settled by the results that follow), DeleteMessage force, and the resource tree stopping at self-typed fields.'
labels:
    repo: core
    topic: ai
---

# Tools and tool calls

## Tool

- `malonaz.ai.v1.Tool` is both what a generation is given inline and a stored resource on `AiService` (generated
  Create/Get/BatchGet/Update/Delete/List; hard delete). Platform tools `tools/{tool}` are the ai processor's alone;
  `organizations/{organization}/tools/{tool}` are an organization's. Ids are kebab-case like every resource's; a stored
  tool's `title` (required on Create, `[A-Za-z][A-Za-z0-9_-]{0,63}`) is the name a model calls it by, and a consumer
  passes it to a generation inline under that name. An inline tool's `name` is that name, as before. Names given to
  one generation must be distinct: the consumer's concern.
- A tool describes; it never says where a call goes. A consumer runs a tool from its own configuration (a method of a
  service it routes, as the caller), so nothing written into a tool can direct a call.
- Sets are not resources: tools sharing the label `ai.malonaz.com/tool-set` under one parent (a mirrored service's
  full name lowercased, dots as dashes); a consumer builds the model-facing `ai.v1.ToolSet` and its discovery tool.
- Rich facts ride as one typed annotation, `ai-engine.malonaz.com/method` = `ToolMethod` (base64 proto, read with
  `aipb.Annotations.Method.Get`), only what callers act on: `service` and `method` (what to call: the parser, the
  agent service), `service_description` (the discovery tool over the set), `read_only` (approval defaults, the
  deprecated string), `request_id` (the caller derives it). Add a fact when a consumer reads it, not before. The builder
  (`go/ai/tool`) writes it on every method tool. `grpc-method` and `no-side-effect` strings are still stamped beside
  it, deprecated, because shipped kenji/kanshi bundles (`app/core/agent/executor.ts`) route and gate on them: remove
  them with kenji's version floor. New code reads only `ToolMethod`. Anything to filter on is a label, not an annotation.
- `go/aip` resource trees skip a field whose type is its own message's (a JSON schema's `items`): without it the
  `json_schema` column's filter/update tree grew exponentially and the server hung at startup, silently.

## The ai processor (`go/ai/ai_processor`)

- The only thing that reflects. `--ai-processor.target pkg.Service=endpoint` (repeatable; `http://` for plaintext),
  `--ai-processor.interval` (5m). Each pass, per target: reflect, build each unary method's tool as `tools/{service}-{method}` in kebab
  case, titled as models always called it (`ProjectGateway.ListQuotes` is `tools/project-gateway-list-quotes`, title
  `ProjectGateway_ListQuotes`; two services sharing a short name fail the later target), request_id masked out,
  description = the method's full name when reflection carries no comments; converge it (Create with request id
  `uuidv5(ns, name)` returns the stored tool, then Update only the differing paths, pinned to its etag), then delete the
  set's platform tools the pass did not write. An unreadable target is logged and counted
  (`ai_processor_target_errors_total`) and its tools left as they were. The target must serve reflection
  (`--…-grpc.enable-reflection`). The sat's `cmd/ai-service` mirrors the ai service itself.

## ToolCall

- The block value is `ToolUse` (renamed from `ToolCall`: fields, numbers and `Block.tool_call` unchanged, so stored
  messages and the wire are byte-identical). The resource `ToolCall` is
  `organizations/{o}/users/{u}/chats/{c}/messages/{m}/toolCalls/{n}`, n the call's position among the message's tool
  calls from 1 (derived, so no provider id shape leaks in); `call_id` is the provider's id, which results name.
- Written only by the AI service, inside `CreateMessage`, in the message's own transaction (`store.InsertMessage`,
  hand-written beside the generated SQL): the generated create prepares the message (its `validate_only` answer,
  stamped in place on the caller's message), then one transaction inserts the message, its assistant tool calls
  (PENDING, or settled at once when the generation answered the call in its block) and settles the chat's pending
  calls its tool results answer (SUCCEEDED/FAILED + `error`, `result_message`). A failed generation's partial
  message makes no calls. Rows derive at creation only: an UpdateMessage rewriting blocks leaves them as created.
- `ListToolCalls` across chats: parent `organizations/{o}/users/{u}/chats/-/messages/-`, e.g. `state = STATE_PENDING`.
- A message with tool calls is deleted only with `force` (`DeleteMessageRequest.force`); its calls are tombstoned
  with it.
