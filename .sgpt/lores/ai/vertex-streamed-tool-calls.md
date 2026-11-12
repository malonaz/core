---
title: 'ai: Vertex streamed tool-call arguments (partial args) and tool schemas'
description: 'What Vertex sends when stream_partial_tool_calls is on (opening chunk, partialArgs by JSON path, closing empty functionCall), how it aborts a call (MALFORMED_FUNCTION_CALL, no closing chunk), the unquoted dotted map keys, the measured abort rate, and why tool schemas go out through ai.JSONSchemaMarshal.'
labels:
    repo: core
    tags: ai,google,vertex,gemini,tool-calls,json-schema
    topic: ai
---

`stream_partial_tool_calls` makes the Google provider set Vertex's
`streamFunctionCallArguments` (Vertex only; the Gemini API has no such mode). The code is
`go/ai/ai_service/provider/google/generate_message.go` and `provider/tool_accumulator.go`.

## The stream, as captured (gemini-3.8-flash, 2026-11)
1. Opening chunk: `functionCall: {name, id, willContinue: true}` (it carries the thought signature).
2. Argument chunks: `functionCall: {partialArgs: [{jsonPath, stringValue|numberValue|boolValue|nullValue,
   willContinue}], willContinue: true}`. A string comes in pieces on the same path and ends with an
   empty `stringValue`. Paths come in alphabetical order at every level. A container is announced
   *after* its children: `$.report.labels{}`, `$.items[]`, no value.
3. Closing chunk: `functionCall: {}`. Nothing else marks a call complete; there is no final `args`
   snapshot.
4. Last chunk: an empty text part, `finishReason: STOP`, the usage.

## Aborted calls
Vertex validates a streamed call as it goes and drops one it judges malformed: the stream ends with
`finishReason: MALFORMED_FUNCTION_CALL`, **no closing chunk**, and a usage with no candidate
tokens. The provider fails such a generation (`UNAVAILABLE`, error info `TOOL_CALL_UNFINISHED` with
the tool and finish reason): the arguments streamed so far are not a call. A stored assistant
message with a tool call and 0 output tokens is one emitted before that guard existed.

Measured by replaying real generations: one report edit (an `UpdateReportRevision` of ~250 output
tokens) aborted in about half the generations with a correct schema, in nearly all with the old
snake_case one, and never with partial args off. Two other asks (a `CreateReport` with labels, a
revision with column formats, up to 1,400 output tokens) never aborted in 16 streamed generations.
What sets the edit apart is not identified. A caller that needs every call to land turns
`stream_partial_tool_calls` off: retrying the same context mostly aborts again.

## Dotted map keys
Vertex writes a map key into the path unquoted: `$.report.labels.analytics.onikisu.com/folder`.
The accumulator takes the tools' schemas (`NewToolCallAccumulator(tools...)`) and, under a map of
scalars, reads the rest of the path as the key (`mergeMapKey`). A map of messages keyed with dots is
still read as nesting.

## Tool schemas
`malonaz.json.v1.Schema` marshaled under its proto field names gives `additional_properties`,
`max_length`, `min_items`: not JSON Schema keywords, so every provider ignored them and no model saw
what a map holds. Anything sending a schema to a provider goes through `ai.JSONSchemaMarshal`
(camelCase keywords, compacted). Tool results that quote a schema (discovery) keep the proto names:
they are stored history and must replay byte for byte (`lores/ai/replay-determinism`).
