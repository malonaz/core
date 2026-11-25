# AI API v2: tools and tool calls as resources

Status: direction agreed, design paused, nothing of v2 built (2026-11-25). This document records why a v2 of the AI
API is wanted, the philosophy it must follow, everything that was tried on the way, and the traps found. It is the
starting point for whoever resumes the design; it is not the design itself. Sections marked *draft* hold proposals
nobody has reviewed.

## 1. Why

The agent framework (`AgentService`, core PR #147, hosted by onikisu as kantoku) runs agents server-side: generate,
call tools, repeat, with nobody watching. Building it exposed that the AI API treats tools and tool calls as values
passed around, never as things that exist:

- **Tools exist only inside a request.** Every consumer that wants "the tools of this gRPC service" reflects the
  service itself and rebuilds them (the ai-engine, the agent service, each client through the engine). Nothing can
  list, filter, label, curate or share a tool.
- **Tool calls exist only inside messages.** A call lives in an assistant message's block and its result in another
  message, joined by the provider's id string. "Which calls are waiting on someone, across all my agents" cannot be
  asked; the agent service keeps its own list on the agent's latest turn, and clients keep theirs in memory.
- **Facts about a tool are strings.** Ten `ai-engine.malonaz.com/*` annotations (`grpc-service`, `grpc-method`,
  `no-side-effect` as `"true"`, `tool-type`, `generation-field-mask` comma-joined, ...), each read by string
  comparison in Go and TypeScript.
- **A product cannot own tools.** Kantoku wants tools that exist only for kantoku (typed tools over its own
  document-backed objects such as plans); there is nowhere to put them.

The author's experience, which is the argument for doing this at all: every time something became a real AIP
resource, filters, labels, masks, search, generated stores and hooks, and warehouse replication made possible things
nobody had planned. Concretely here: one filtered `ListToolCalls` is the cross-agent attention queue; audit and
analytics of tool use come from the replicated table; a generation can be given stored tools by name.

### What is not clean in v1 (from an audit of `malonaz.ai.v1` and its service)

Matters:

1. `ToolCall` and `ToolResult` are values embedded in message blocks; a call and its result live in two messages.
2. Pending, answered and server-answered calls cannot be listed: the state is spread across `tool_call.result`,
   tool messages and client memory.
3. `ToolResult` repeats `tool_name` and `tool_call_id`, and has its own annotations beside the call's.
4. `ToolCall.result` means "answered by the server" while client answers live in tool messages: two homes for one fact.
5. `ROLE_TOOL` messages carry nothing a model needs beyond the calls' results; the service re-pairs them with their
   calls at render anyway (`pairToolCalls`).
6. Tools exist only as per-request values; every consumer rebuilds them by reflection.
7. The stringly annotations above.
8. `ToolSet` is a client-built value carrying a client-built discovery tool; membership and discovery state ride
   annotations.
9. Whether a tool is read-only is a string annotation, not a fact of the tool.
10. `UpdateChatRequest` and `UpdateMessageRequest` open output-only fields (`price`, `model_usages`, `status`) so the
    service can write its own projections through its public RPCs.
11. `GenerateMessage` appends any non-assistant input, so tool results ride input messages and are flagged, excluded
    and resent when a generation fails.
12. `TextToText` and `TextToTextStream` are deprecated shims with their own copies of types and no caller.

Cosmetic: "partial" said twice (`partial_tool_call` and `ToolCall.partial`); `ToolCall.extra_fields` read but never
written since 2026-01; an unused `ai.v1.JsonSchema`; engine parse shapes in the resource package; scrambled `Message`
field numbers; `GenerateMessageResponse.model_usage` duplicating the message's; inconsistent streaming RPC names;
`CreateModel` / `CreateVoice` declared with no caller; `Chat.tool_message_count`.

Already clean and to be carried over unchanged: `Chat` (bar the count), `Decision`, `Model` and its configs, `Voice`,
`ModelUsage`, the stats and metrics messages, images and documents, the generation configuration messages.

## 2. Philosophy

These were decided by the author in review and bind any v2. Each was learned by getting it wrong first (section 4).

1. **A tool is a plain resource.** It describes: what it is called, what it does, what it takes. It never directs:
   no address, no executor, no "how to run me" modelled in it. What a consumer does with a tool is the consumer's
   business and comes from the consumer's own configuration. This is also the security property: nothing a client
   writes into a tool can send a call, or the owner's token, anywhere.
2. **Custom facts go in annotations, as one typed message per concern.** Not several strings to assemble. The first
   is the method annotation: which gRPC method a tool stands for.
3. **Annotations are not filterable; labels are.** Anything that must be selected on is a label with a declared key,
   defined deliberately and only when something needs it. The annotation holds the rich facts; a label duplicates
   only what a filter needs, written by the same writer from the same source.
4. **There is no tool-set resource.** A set is the tools sharing a label, plus the discovery tool the engine builds.
5. **One thing reflects.** A processor mirrors the configured services' unary methods into tools and reconciles them
   (the house pattern for mirroring an external source: deterministic ids, converge, delete what disappeared).
   Everything else reads tools from the API.
6. **The schema comes from the proto.** The engine generates a tool's JSON schema from a named message; a schema is
   written by hand only when no proto describes the arguments.
7. **`ToolCall` is the name of the resource.** Not `ToolInvocation`. It is a child of its message, with ordinal ids
   (`.../messages/{message}/toolCalls/1`): derived from position, so writes are idempotent and independent of
   providers, whose call ids differ in shape and are not always present or unique.
8. **A result is typed and lives on its call.** `ToolResult`, the API's own type, as a field of `ToolCall`; never a
   status plus a string pointing elsewhere. Pending is the absence of a result: no state enum. A singleton child
   resource for the result was considered and rejected (a result does not exist until settled, has no life of its
   own, and splitting it loses the trivial "pending" filter; read masks already keep large results out of lists).
9. **Calls are written with their message, in one transaction.** A persisted call that can be missing until a retry
   heals it is not acceptable: queues and audit read these rows.
10. **Ids are kebab-case like every other resource.** The name a model calls a tool by is a separate field, so
    mirroring `ProjectGateway.ListQuotes` as `tools/project-gateway-list-quotes` changes nothing a model sees.
11. **Nothing without a reader.** Every field, method and option has a consumer today or is intrinsic to the
    resource. Rejected in review as speculative: an AIP method-kind enum, resource type and patterns, pagination /
    filter / mask capability flags, a `request_id` hint and a service description on the method annotation.
    v2 is the same capabilities in a clean shape, not new features.
12. **Shipped clients keep working.** Kenji is a native app with old builds in contractors' hands for weeks. An
    internal refactor must not stop it routing or gating tool calls, nor force an update gate. This is what keeping
    v1 alive is for.
13. **One word, one thing.** `name` is a resource name. `ToolCall` is the resource. `result` is a `ToolResult`.

## 3. The target, as far as it is decided

Sketches, not final protos.

```proto
// A tool the model can be given. Platform tools are written only by the processor.
message Tool {
  string name = 1;         // tools/{tool} | organizations/{organization}/tools/{tool}, kebab-case id
  // the name a model calls it by (see open question 1), description, JSON schema of its arguments
  map<string, string> annotations;  // typed messages under declared keys; inherited by calls
  map<string, string> labels;       // e.g. the tool-set label
  // create_time, update_time, etag
}

// On a tool standing for a gRPC method, and the calls made to it.
message ToolMethod { string service = 1; string method = 2; }   // read-only: see open question 2

// One call a generation made. Written with its message.
message ToolCall {
  string name = 1;         // .../chats/{chat}/messages/{message}/toolCalls/{position}
  // the tool called, the provider's call id, arguments (Struct), inherited annotations, labels
  ToolResult result;       // unset while pending
}
```

- Listing: pending calls are `NOT result:*` (a partial index serves it), failed ones `result.error:*`.
- The end state has a message referring to its tool calls rather than embedding copies.
- Two parents for tools: platform-wide (`tools/*`, the processor's) and an organization's own, so "these tools exist
  just for kantoku" is decided by authorization, not by a label convention.

## 4. What was tried, and what each attempt taught

In order. The cost of this history was two days; do not repeat it.

1. **Named tool targets** (a registry of named endpoints agents point at). Rejected: a service's full name is already
   its name; a new noun for nothing.
2. **A derived catalogue in the agent service.** First as a `Capability` resource, then `ToolService`, then
   `ToolSet` / `Tool` with four RPCs computed on every read from reflection ("derive, don't store"). Built and
   shipped on the agent branch; it works and is what kantoku's picker reads today. Rejected as the end state: it makes
   "a routed gRPC service" the concept, cannot hold a product's own tools, and puts a second `Tool` beside the AI
   service's. Lesson: the author asked for stored resources three times before it was designed that way; "nothing
   stored that can be derived" is a good default and the wrong rule when being a resource is the point.
3. **Stored tool sets beside the derived catalogue**, with a closed set of "executors" a tool picks from. Rejected:
   executors are a concept in the schema for something that belongs in the consumer (principle 1).
4. **`Tool`, `ToolSet`, `ToolCall` as AI-service resources.** `ToolSet` dropped (principle 4).
5. **`ToolInvocation`** as the resource's name, to dodge the existing `ai.v1.ToolCall` value. Rejected (principle 7).
6. **A first `ai.v2`** holding only the new resources beside v1, as a second service over the same database.
   Abandoned within the hour for (7); its one real flaw was writing calls and their message through two stores with
   no shared transaction.
7. **In place in `ai.v1`** (core draft PR #200, branch `ai-tools`; onikisu branch `ai-tools`). Built end to end:
   `Tool` a resource in place, the processor, `ToolCall` persisted in the message's transaction, legacy strings still
   stamped for kenji. It proved the mechanics and is the source of most gotchas below. Rejected on reading the protos,
   because in-place constraints leave warts that are permanent:
   - `Tool.name` is a resource name when stored and the model-facing name when inline, with `title` as a second
     model-facing name for stored tools; every consumer must know which applies.
   - The block value had to be renamed `ToolUse` so the resource could be `ToolCall`; the block field is still
     `tool_call`, because renaming it changes stored JSON.
   - `ToolCall.tool_call_id` is the provider's id, not the resource's.
   - `ToolResult` repeats what its `ToolCall` already says.
   - "Tool set" is expressed twice: the new label and the engine's old `tool-set-name` annotation.
   - Messages still embed the calls and results the resources now also hold.
   Within it, two shapes were built and thrown out: a `ToolMethod` annotation with thirteen fields (principle 11), and
   a `ToolCall` with `google.rpc.Status error` plus `result_message`, a string naming another message, and no
   arguments (principle 8). The second came from "do not store anything twice" applied without reading the API's own
   types.
8. **`ai.v2`, the whole API, clean, with v1 kept alive.** The current direction. A proto draft was started and
   paused; its unreviewed proposals are in section 7.

Process lessons, since they caused as much rework as the design did: read the protos, not the summary of them; a
report that says "tested" says nothing about whether the shape is right; and speculative fields arrive through the
brief as easily as through the code.

## 5. Gotchas

Found by building (7). Verified in code or by test unless marked.

**Stored data and the wire**
- `Message.blocks` is JSONB written with proto field names; unknown fields are discarded on read; `role` is stored
  as the enum number. Renaming a field of a stored message changes its JSON key; renumbering does not matter there
  but does on the binary wire. A block that changes from an embedded object to a reference under the same key cannot
  be read by protojson without converting the stored rows.
- In `ai.v1.ToolCall` (the value) `name` is the tool's name and `id` the provider's; in `ToolResult` the same id is
  `tool_call_id`. Any resource built on these names inherits the confusion.
- The service already renders results in call order right after their assistant message (`pairToolCalls`), whatever
  message they arrived in. So "results live on calls" can be made byte-identical to what providers see today.

**Shipped clients**
- Kenji's Harper gets its tools from the server's ai-engine and reads `grpc-method` to route each call and
  `no-side-effect` to decide approval (`app/core/agent/executor.ts` in onikisu). Remove either string and an old
  build cannot call methods at all.
- Tool calls stored before any change carry only the legacy strings; history must keep rendering and deciding
  approval from them. A reader that falls back to nothing breaks every old chat.
- Old kenji reads tool messages and filters on `status` and the `superseded` label; v1 must keep serving those.
- Kanshi tailors schemas per method (field masks, depth) through the engine, so the engine keeps building inline
  tools for it whatever the processor stores.

**Codegen and stores**
- Two generated stores cannot share a transaction. Writing a message with its calls needed a hand-written
  `InsertMessage`, fed by the generated create's `validate_only` answer.
- A child collection forces `force` on the parent's Delete (`DeleteMessage` for a message with calls).
- Generated joins run on every Get, BatchGet and List and ignore the read mask; a join can only bring a whole row;
  join filters cannot test presence on a JSONB field; a hand-written `RETURNING` on a table with joins must list the
  join columns.
- A resource whose message contains a field of its own type (the recursive JSON schema) made the AIP resource tree
  recurse forever and the server hang at startup with no output. Fixed on the `ai-tools` branch only.
- Core commits its generated code. After a proto or model change run the copy step, or dependants build against stale
  files while core's own tests stay green.
- Label values are lowercase and at most 63 characters, so a service's full name becomes a label value lowercased with
  dots as dashes.

**Invocation**
- Calling a gRPC method dynamically needs the method's descriptors, not only a JSON schema. A JSON codec on core's
  servers was tried on paper and does not work: tool schemas use a model-friendly JSON (money as `"USD 25.50"`,
  snake_case field masks, Go durations, intervals, nested messages sent as strings) that only the descriptors can turn
  into a request. So a consumer that runs method-backed tools still obtains descriptors (by reflection, behind one
  function, on the agent proposal branch). "Nothing else reflects" holds for learning what tools exist, not yet for
  invoking them.

**Tools**
- Names a model sees must be unique within one generation; that is the consumer's concern. The processor fails a
  target if two services would produce the same short name.
- A method with no comment in reflection gets its full name as its description; targets need reflection enabled.
- In-process calls skip the authentication interceptor, so a generation given stored tools by name must itself check
  they are platform tools or the chat's organization's.
- The warehouse reads `Chat.tool_message_count` (`dim_harper_chats`).

## 6. The agent side

- What runs today is entirely on v1 and does not wait for v2: core `agent-v1` (PR #147), onikisu `kantoku` and
  `kantoku-ui`. It includes token exchange (an agent calls its tools and generates as its owner), the derived
  catalogue of (2), `ToolGrant` naming a set or a tool, `ApproveToolCall` (with always) and a total `ResolveToolCall`,
  `AuthorizationService.TestMethods` trimming the catalogue and each turn's toolbox to what the owner can call, lease
  fencing and caps.
- v2 replaces one layer of it: how a turn's toolbox is built (from `ListTools`), what a grant names (a stored tool, or
  a selector on the tool-set label), approvals addressing a `ToolCall`, and where the attention queue reads from. Do
  not invest further in the agent service's catalogue or grant shapes until then.
- A proposal branch (`agent-v1-tools`, core) built that adoption against the in-place attempt: toolbox from stored
  tools, the agent catalogue and its four RPCs deleted, `ToolSetSelector{parent, id}`, reflection kept for invocation
  only. Its proposal for waiting calls: approve and resolve stay methods of the agent service (so the agent resumes
  at once and the AI service learns nothing about agents) but address the `ToolCall` by name; "awaiting approval" or
  "awaiting a result" is a label the agent service sets on the call, re-written on each claim so it heals after a
  crash. Unreviewed.
- Kantoku's own tools (typed tools over document-backed objects, generated from their protos, run as the owner
  against the platform document API, working with no client connected) wait for stored tools.

## 7. Draft proposals from the paused v2 design (unreviewed)

Recorded so they are judged rather than rediscovered.

- v2 is the source of truth; v1 becomes a stateless adapter over v2's API in process, deleted whole at the end. This
  follows authentication's move (v2 beside v1 with equivalence tests, then v1 deleted).
- v2 has no tool messages: a result is written on its call, and v1's adapter derives tool messages for old clients.
- One conversion at cutover, of tool-bearing message rows only, because v2's block holds a reference where v1 rows
  hold an object under the same key. This conflicts with the wish to rewrite no stored messages (open question 4).
- The model-facing name as `function_name` on both inline and stored tools; `read_only` as a field of `Tool`; a
  `ToolCall` naming its `Tool`; a second typed annotation for the arguments' proto message and field mask.
- A generation is given `tools` (stored, by name), `inline_tools`, and discovery tools.
- Not carried to v2: the `TextToText` shims, `extra_fields`, the unused JSON schema message, unused RPCs.

## 8. Open questions

1. The field holding the name a model calls a tool by. The author chose `title` during the in-place review; the
   draft proposes `function_name`. It must be one field for inline and stored tools alike.
2. Where read-only lives. Recommended: a field of `Tool`, since it is true of any tool, not only method-backed ones.
3. Scope and order: the whole API designed on paper and built in slices starting with tools, tool calls and messages
   (assumed), or tools and tool calls alone first.
4. Whether one conversion of stored tool-bearing messages is acceptable, or v2 must read v1's stored shape as is.
5. Whether v2 drops tool messages.
6. How a consumer invokes a method-backed tool without reflecting (section 5).
7. Whether the engine's remaining string annotations (`tool-type`, `proto-message`, `generation-field-mask`, the
   discovery ones) become typed, and what happens to its `tool-set-name` beside the tool-set label.
8. How kantoku registers its own tools (a deploy step under a service account was proposed) and what the typed
   annotation for a document-backed tool holds.
9. Labels for analytics by method or resource: none until a query needs one.

## 9. Where things are

| What | Where |
|---|---|
| In-place attempt, core | draft PR #200, branch `ai-tools` |
| In-place attempt, onikisu (processor hosted, grants, TypeScript readers) | branch `ai-tools` |
| Agent service adopting stored tools, proposal | core branch `agent-v1-tools` |
| The agent framework on v1 | core PR #147 (`agent-v1`); onikisu `kantoku`, `kantoku-ui` |
