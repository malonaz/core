---
title: 'ai: the OpenAI provider (Responses API, reasoning replay, usage, model lifecycle)'
description: 'How ai_service talks to OpenAI: the openai provider generates messages through the Responses API while the OpenAI-compatible providers sharing its Client (groq, cerebras, xai, moonshot, baseten) stay on Chat Completions; stateless requests (store false) with encrypted reasoning kept on thought blocks and replayed; what a block stores of an output item; reasoning efforts per model in provider_settings; disjoint usage with cache writes; strict_tool_names; the model lifecycle facts of 2026-11 (deprecated models dropped, gpt-transcribe for speech-to-text, no OpenAI text-to-speech until it is built on the Realtime API); SDK and API traps; how it was verified.'
labels:
    repo: core
    tags: ai,openai,responses,reasoning,prompt-cache,providers,models
    topic: ai
---

Code: `go/ai/ai_service/provider/openai/`. Models: `provider/configs/openai.json`.
Decisions (`decision.go`) are a separate, plain HTTP path: see `lores/ai/decisions`.

## Two generation paths, one Client

`openai.Client` serves OpenAI and every OpenAI-compatible provider. `config.ResponsesAPI` picks the
path in `StreamGenerateMessage` (`openai.go`), the only place that knows there are two:

- `responses.go`: the Responses API, for `openai` only.
- `chat_completions.go`: Chat Completions, for groq, cerebras, xai, moonshot, baseten. Unchanged by
  the move, and it no longer has any OpenAI-specific branch.
- `content.go`: what both share (image URLs, data URLs).

The Client also transcribes speech (`speech_to_text.go`) and answers decisions (`decision.go`).

Why OpenAI left Chat Completions (checked against the API, 2026-11-12):

- GPT-5.6 and GPT-6 models refuse function tools there unless `reasoning_effort` is `none`.
- The pro models are served on Responses only.
- It never returns reasoning, so nothing can be shown or carried to the next turn.

## The request

- **Stateless**: `store: false`. We replay our own history every turn, a chat can fork, change
  model or provider, and nothing of ours is retained at OpenAI. `previous_response_id` would bind a
  chat to responses stored there, which OpenAI only promises to keep for 30 days.
- `include: ["reasoning.encrypted_content"]`: reasoning items come back with their encrypted
  content. It is the default when stateless; it is sent anyway because replay depends on it.
- `reasoning.summary: "auto"` on reasoning models: the summary is the thought text we show.
  OpenAI's docs say an organization may need to be verified to get summaries; ours is.
- Tools are sent with `strict: false`. Omitting it means strict mode, which rewrites the schema
  (`additionalProperties: false`) and rejects optional properties.
- System messages are `system` input messages, in place. Images are `input_image`, documents
  `input_file` (`file_url` for a URL: no download, unlike Chat Completions).
- `temperature` is only accepted when the effort is `none`; anything else is a 400, passed on as
  `InvalidArgument`. `max_output_tokens` below 16 is a 400 too.

## Blocks and output items

A block is one output item, at the item's `output_index`:

| Output item | Block | Kept for replay |
| --- | --- | --- |
| `reasoning` | thought: the summary parts joined by a blank line | `signature` = `encrypted_content`, `extra_fields.id` = the item id |
| `message` | text (a refusal is text, with `STOP_REASON_REFUSAL`) | `extra_fields.phase` (`commentary` or `final_answer`, GPT-5.4 and later) |
| `function_call` | tool call, id = `call_id` | nothing |

Replay (`pbMessageToResponseItems`), one input item per block, in block order:

- A thought goes back as a `reasoning` item only if it has both an id and a signature, with
  `summary: []`. The summary is not rendered to the model (the input token count is the same with
  the real one, an empty one or a made-up one); `summary` itself is required.
- Text goes back as an assistant message with its phase. OpenAI asks for the phase to be kept.
- Item ids of messages and function calls are not stored: OpenAI accepts items without them. The
  reasoning id is, because the SDK always serializes it.
- Reasoning made by one model family is accepted by another, and by a model that does not reason:
  the API leaves out what the model cannot use. No error.
- History from Chat Completions or from another provider is text and tool calls, which replay as
  they are. Tool call ids of any shape (`toolu_...`) are accepted as `call_id`.

Thoughts never cross providers: `withoutForeignThoughts` in the service layer (see
`lores/ai/replay-determinism`). Without it, another provider's thought reaches this path as a
reasoning item with no id, a 400.

## Reasoning efforts

Each model's `provider_settings` maps every `ReasoningEffort` name to the effort it runs at
(shared jsonnet locals at the top of `openai.json`). A level the settings leave out is not sent,
and the API picks.

- Unspecified is the least a model can reason (`none`; `low` for gpt-6-astra and gpt-6.1-sol, which
  cannot turn it off; `medium` for the pro models). OpenAI's own default is `medium` from GPT-5.5
  on: callers who never asked for reasoning would pay for it.
- A level a model lacks runs at the nearest it has (`MAX` is `xhigh` before GPT-5.6).
- Accepted values: 5.2, 5.4, 5.4-mini, 5.5: none, low, medium, high, xhigh. 5.6 family, 6-luna,
  6-sol, 6.1-terra: those and max. 6-astra, 6.1-sol: low to max. 5.2-pro, 5.4-pro, 5.5-pro: medium,
  high, xhigh.
- To learn what a model accepts for free: send an effort it cannot have (`minimal`). The 400 lists
  the supported values, and a 400 is not billed.
- Effort is adaptive: a simple prompt at `high` often reasons for zero tokens and returns no
  reasoning item.

## Usage

`responseModelUsage` makes the categories disjoint: `input_tokens` includes `cached_tokens` and
`cache_write_tokens`, `output_tokens` includes `reasoning_tokens`.

- From GPT-5.6 on, a cache write is billed 1.25x the input price (30 minute lifetime) and a read
  0.1x (0.05x on GPT-6.1). Caching is implicit, with the breakpoint at the end of the latest
  message, so a multi-turn chat writes each new turn and reads the rest. Seen on gpt-6-luna with a
  3,100 token prefix: turn 1 writes 3,100, turn 2 reads 3,100 and writes 16.
- Before GPT-5.6 no cache write is reported or billed: only reads, from the second turn.
- Reasoning tokens are billed as output, and priced so (`SetModelUsagePrices` falls back to the
  output price).
- Not modelled: requests over 272K input tokens cost 2x input and 1.5x output on the 1.05M context
  models; Batch, Flex and Fast tiers (we send no `service_tier`); the 12 hour cache lifetime.

## Tools

- OpenAI only calls tools it was given: after a discovery, the model cannot call the discovered
  tool by name. Every OpenAI model has `strict_tool_names`, which gives it `execute_tool`.
- Parallel calls are on by default. `StreamPartialToolCalls` sends a partial when a call starts
  and on every arguments delta.

## Model lifecycle (2026-11-12)

- `GET /v1/models` returns a `shutdown_date` per model; `deprecate_time` in the config is that date,
  and the service refuses the model from then on.
- No model with a shutdown date is configured. Dropped: gpt-5-mini, gpt-5-nano, gpt-5-pro
  (2026-12-11), gpt-4.1 (2027-01-14), gpt-5.1 (2027-04-01), `whisper-1` (2027-02-26), `tts-1` and
  `gpt-4o-mini-tts` (2027-01-06). The text models all worked on the Responses path.
- Speech-to-text is `gpt-transcribe`, on the endpoint `whisper-1` used: `speech_to_text.go`
  (the `sashabaranov/go-openai` client), shared with Groq's whisper models.
- **There is no OpenAI text-to-speech.** Its models have no successor on `/v1/audio/speech`:
  OpenAI points to `gpt-realtime-2.1-mini` on the Realtime API, a session over WebSocket (configure
  the voice and PCM output, send the text as a user message, read base64 audio deltas) with a
  conversational model, not a drop-in. Nobody has built that. The client's text-to-speech code
  went with the models, and with Groq's unused `playai-tts`, the last one that reached it: use
  Cartesia, ElevenLabs or Deepgram.
- gpt-5.6-sol is at a promotional price until 2026-11-30; the price after is not published.
- Docs serve markdown: `developers.openai.com/api/docs/models/{model}.md`, `/pricing.md`,
  `/deprecations.md`, `/guides/{reasoning,prompt-caching,preserved-reasoning,migrate-to-responses}.md`.

## Traps

- **Prefix changes drop reasoning.** OpenAI documents that newer models drop the reasoning items
  that follow a change in the history before them, silently (reported in `input_transformations`,
  which openai-go v3.98.0 does not expose). Changing `reasoning.effort` or the tool list in the
  middle of a chat counts. Not seen on gpt-5.6-luna or gpt-6-luna: after an edited history the input
  token count still included the reasoning. Treat it as a cost, not a failure. Reformatting a
  function call's arguments (whitespace, key order) does not change the token count either: OpenAI
  renders them itself.
- **SDK error text.** Since openai-go v3.98.0 an API error prints as `OpenAI API error: 400 Bad
  Request`. `apiError` puts the provider's answer back and codes the error from the HTTP status,
  for both paths. A provider whose error body is not OpenAI's shape (Baseten answers
  `{"error": "..."}`) still surfaces as a JSON decoding error without the status.
- **Chat Completions tool calls split in two** when a provider streams the arguments in several
  chunks and repeats the id only in the first: the path keys calls by id. OpenAI did that, so its
  tool calls were broken there, and chats that hold such a call (a call with an empty name) fail on
  either path. Groq, Cerebras and xAI send whole calls and are fine. Left as it was.
- An effort the settings do not map is not an error: check the config when a level seems ignored.

## Verifying against the real API

Same harness as `lores/ai/decisions` (a copy of the sat with real keys, run by hand).

- The sat accepts several SUTs: start a second `ai-service` built from another revision on another
  port, against the same database, to continue an old binary's chat on the new one.
- Its gRPC client gives unary calls 10 seconds: set a deadline on the context.
- Pick prompts that need the tool result to reason (a word problem using the weather), or the
  model will not reason and the replay of reasoning goes untested.
