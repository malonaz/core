---
title: 'ai: chat replay must be byte-stable (thinking-block binding)'
description: 'Why ai_service must send a chat''s history as the exact same bytes on every turn: Anthropic binds thinking blocks to the conversation before them (fable-5.2+), and protojson output changes with the server binary, so any deploy breaks replay of structured tool results. ai.JSONMarshal and the prefix-stability rules.'
labels:
    repo: core
    tags: ai,anthropic,thinking,protojson,replay,prompt-cache
    topic: ai
---

`ai_service` rebuilds the provider request from stored messages on every turn
(`go/ai/ai_service/generate_message.go`). Whatever it sent at turn N must reappear
**byte for byte** as the start of turn N+1's request.

## Why
- Anthropic binds each thinking block's signature to the conversation before it
  (claude-fable-5.2+). Any byte difference earlier in the history fails the request with
  400 `Invalid signature in thinking block ... bound to a different conversation ...
  first at messages.X.content.Y`. Stored history never changes, so that chat then fails
  on every turn.
- Gemini thought signatures and the prompt cache depend on the same stability.
- OpenAI does not fail: it drops the reasoning that follows a changed history, and the model
  reasons again at our cost (`lores/ai/openai`).

## The protojson trap
`protojson` (and so `pbutil.JSONMarshal`) adds or drops whitespace depending on a hash of
the running binary (`google.golang.org/protobuf/internal/detrand`). Its output changes
after **any** rebuild. Structured tool results (`ai.ParseToolResult`) and tool call arguments
are rendered with it at every request, so a deploy in the middle of a chat changes every
tool result's bytes. Rolling deploys make it worse: requests can alternate between old and
new binaries.
Seen on staging 2026-11-10 (chat `cc9102b9`): the history was clean and 40+ turns
worked; it broke when a rolling deploy started serving a new binary.
Anything rendered for a model goes through `ai.JSONMarshal` (protojson + `json.Compact`), never
`pbutil.JSONMarshal`. Only the AI path needs this: elsewhere protojson's instability is intended.

## Rules for the provider-bound history
- Render deterministically: canonical JSON, a fixed message order (`create_time` plus a
  tie-breaker).
- Store repairs instead of recomputing them: a stand-in result for a missing tool call
  (`pairToolCalls`) must be persisted once and replayed verbatim, never replaced later.
- Leave out messages only at the end of the history (failed inputs, superseded forks).
- Store provider blocks verbatim: thinking text + signature, `redacted_thinking` data, OpenAI's
  encrypted reasoning. UI placeholders never go back to the provider.
- A provider is only sent its own thoughts and signatures (`withoutForeignThoughts`): they are
  bound to their issuer, and Anthropic, Vertex and OpenAI all reject another's. Its view of its
  own turns is unchanged, so a chat can change provider and come back.
- No `drop_block` (`thinking.block_binding.prefix_mismatch_behavior`): it would hide drift. Fix the
  drift instead.
