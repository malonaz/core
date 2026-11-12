---
title: 'ai: decisions (the Decision resource and the OpenAI, Gemini and TypeSafe Jev decision models)'
description: 'The malonaz.ai.v1.Decision resource and AiService.CreateDecision: typed questions (boolean, choice, score) asked of a decision model about a shared input, answered with calibrated probabilities and persisted. The contract (immutable input and questions, service-set answers in question order, request_id idempotency with the lookup before the model call), why there is no confidence field, the limits, DecisionModelConfig, the three providers behind provider.PostJSON and their quirks, the mock script, and a comparison of the provider APIs (wire shapes, limits, media, usage, pricing, Go SDK versions).'
labels:
    repo: core
    tags: ai,decisions,openai,gemini,typesafe,jev,providers,idempotency
    topic: ai
---

A decision model answers typed questions about an input with calibrated probabilities instead of
generating text. `AiService.CreateDecision` asks one and persists the result as a
`malonaz.ai.v1.Decision` (`organizations/{organization}/users/{user}/decisions/{decision}`).
Code: `malonaz/ai/v1/decision.proto`, `go/ai/ai_service/decision.go`,
`go/ai/ai_service/provider/{openai,google,typesafe}/decision.go`; sat in
`go/test/ai/ai_service/sat/decision_test.go`.

## The contract

- The caller owns `model`, `input` (text, image or document blocks) and `questions`, all immutable.
  The service sets `answers`, `model_usage` and `price`, and drops whatever a client sends there.
  Only `labels` and `annotations` can be updated: a decision is a record.
- `answers[i]` answers `questions[i]` and carries its `question_id`. A provider returns its answers
  in question order; the service stamps the ids and refuses a count mismatch (`Internal`).
- Questions and answers are lists, not maps keyed by id: protoc-gen-core cannot store a
  message-valued map as JSON (`map[string]message` in the model, `encoding/json` on proto messages).
- Three kinds: `boolean` (a probability), `choice` (the top option and a probability per option id),
  `score` (the expected level index and a probability per level). An answer may instead be a
  `refusal`. Choice options are a map: providers get them sorted by id, which is also how Gemini
  evaluates them.
- **No `confidence` field.** The providers define it three different ways (below), so only the
  probabilities are comparable across models. Derive whatever statistic a caller needs from them.
- Limits are the intersection of the providers', so a decision runs on any of them: 1-400
  questions, 2-255 options, 2-10 levels, question and option ids `[A-Za-z0-9_-]{1,64}`.
- A model is a decision model when its config has `decision` (`DecisionModelConfig`): context
  limit, the `image` / `document` flags the service checks before calling the provider, and a
  `TttModelPricing`. The pricing is the decision endpoint's, not the model's chat pricing
  (`gpt-6-luna` differs between the two).
- Decisions show up in `ComputeStats` (`StatsSnapshot.decisions`), aggregated by the same query as
  messages (`computeModelStats` over either table).

## Idempotency

The model call precedes the insert, so the generated Create's `request_id` replay alone would pay
the provider again on every retry. `CreateDecision` therefore looks the decision up by
`(parent, request_id)` first (`Store.GetDecisionByRequestID`) and returns it without calling the model.

- A failed model call persists nothing: the retry asks again.
- A retry racing the request it repeats may ask the model twice; the generated insert then returns
  the row that won, so both callers get the same decision. Closing that gap needs a pending state
  or a lock held across the model call: not worth it at a fraction of a cent per call.
- The lookup is scoped to the parent, so a request id never returns another user's decision.
- Callers should derive the request id from what they decide about, so job retries and
  redeliveries reuse the first decision.

## Providers

All three are one JSON `POST` through `provider.PostJSON`, with wire structs next to the mapping,
rather than SDK calls: no go-genai release has Decisions yet, TypeSafe has no Go SDK, and OpenAI's
stays on the same path as the other two by choice (openai-go has `client.Decisions` since v3.73.0,
and core pins v3.98.0). Move Gemini to its SDK when a release has it, if it removes code.

- No retries in a provider. `PostJSON` codes the failure (429 → `ResourceExhausted`, 5xx and
  TypeSafe's 529 → `Unavailable`, other unmapped 4xx → `InvalidArgument`) and the caller retries
  with the same request id.
- OpenAI (`providers/openai/models/gpt-6-luna`): a predicate takes no criteria, so the boolean
  descriptions are appended to the instructions; documents are refused; cached tokens are
  subtracted from `input_tokens`.
- Gemini (`providers/google/models/gemini-decision-1`): the Developer API only, so it needs
  `GOOGLE_API_KEY`, not the Vertex credentials; adjacent text blocks get a newline between them
  because Gemini joins them with nothing; a safety block is a 400 (`InvalidArgument`) for the
  whole request.
- TypeSafe (`providers/typesafe/models/jev-1.14`, `jev-1.13`, key `TYPESAFE_API_KEY`): text only
  (their images are not production-ready), one text is sent as a string and several as a list.
- Mock: the decision carries its own script, `mock.Script(answers...)` in the
  `mock.malonaz.com/script` annotation; `mock-1` is a decision model accepting images.
- A new provider implements `provider.CreateDecisionClient` and registers models with a `decision`
  config. Nothing else changes.

Checked against the real APIs through the service (2026-11-12): create, read back, replay (2-3 ms
against 300-450 ms for the model), stats.

- Gemini: text, an inline and a URL image, a PDF, the 400 of a bad request. The same request
  returned the same probabilities twice.
- OpenAI: every question kind, an inline and a URL image, a refusal on one question while the
  others answered, cache writes then reads in the usage. Its probabilities are rounded to two
  decimals and do reach 0 and 1.
- OpenAI's implicit prompt cache is not free: once the input passes 1,024 tokens (one image is
  enough) the first call is billed as a cache write, 1.25x the input price, and only later calls
  with the same input read it at 0.1x. One-off decisions over large inputs pay the surcharge for
  nothing; `prompt_cache_options.mode = "explicit"` would turn it off, and is not sent today.
- OpenAI validates the whole body (unknown fields, enums, array lengths) before it checks billing:
  a 429 `insufficient_quota` means the request itself was valid.
- TypeSafe: not called, no key.
- To repeat it: run a copy of the sat (other ports, real keys instead of `--ai-service.mock-provider`)
  outside `plz test`, from a directory holding the binaries and migrations under their sat paths.
  The sat's `os.Exit` skips its cleanup, which only the plz sandbox hides: stop the postgres it
  leaves behind (`pg_ctl stop -D /tmp/postgres-*`).

# The provider APIs compared (2026-11-11)

A decision call sends one shared input plus N independent typed questions and gets one typed answer
per question in a single forward pass: no generated text, output tokens always 0, only input billed.
None of the three stores anything: no decision id, no retrieve/list, no streaming, no batch.

## Wire shapes

| | OpenAI | Gemini | TypeSafe (Jev) |
| --- | --- | --- | --- |
| Call | `POST /v1/decisions` | `POST /v1beta/decisions` (Developer API only, **not Vertex**) | `POST https://api.typesafe.ai/v1/systemone` |
| Models | `gpt-6-luna` | `gemini-decision-1` | `jev-1.14.0`, `jev-1.13.0` (aliases `jev-latest`, `jev-preview` move) |
| Input field | `input`: string or items (user/developer/system/assistant messages, function and custom tool calls + outputs) | `input`: string or content blocks | `state`: string, JSON object or array |
| Input media | text, images (data URL or public https), `input_json`; no files, no audio | text, image, audio, video, PDF (inline base64 or Files API / https URI, 15 MB per URL) | text/JSON; PNG/JPEG via the `assets` map + placeholder strings, behind `TypeSafe-Danger-Zone-Images: true` (not production-ready per their docs) |
| Questions | ordered array, optional `name`, 1-400 | map, key `[A-Za-z0-9_-]{1,64}`, 1-1000 | map, no count limit (64k tokens per request, 32k per question; 262k with `TypeSafe-Danger-Zone-Long-Context: true`) |
| Yes/no | `predicate`: instructions only | `noul` + optional `criteria.true_description` / `false_description` | `noul` + optional `criteria.true` / `false` |
| Choice | `choices[]` `{value: string or bool, description?}`, 2-255 | `criteria` map option id → description, 2-255, **sorted alphabetically before evaluation** | `criteria` map name → description or null, 1-255 |
| Score | `levels[]` `{label, description?}`, 2-10 | `criteria` string array, 2-255 | `criteria` array, 1-10 |
| Other kinds | none | none | `box` (bounding box in an image, 1.14 only) |
| Question media | images and JSON parts in `instructions` | text only | images anywhere |
| Context | long-context multipliers apply | 262,144 tokens | see above |
| Caching | prompt caching: implicit breakpoint at the end of `input`, prefix ≥ 1,024 tokens, `prompt_cache_key`; questions are outside the prefix | none | none |
| Price | $0.10 / 1M input (cache read/write at the model's caching rates) | $0.10 / 1M input | $0.042 / 1M input |

## Answers

- Yes/no: OpenAI `probability`; Gemini `noul` + `confidence`; Jev `noul` only.
  **Gemini clamps to 0.01-0.99**: a threshold above 0.99 never matches.
- Choice: the selected option, a probability per option (OpenAI: array of `{value, probability}`;
  Gemini/Jev: map), `confidence`.
- Score: `score` = probability-weighted mean of the zero-based level index (can fall between
  levels), a probability per level, `confidence`; Gemini/Jev also echo a `legend`.
- **`confidence` is not comparable across providers.** Gemini: the top outcome's probability
  (`max(p, 1-p)` for yes/no). Jev: `(p_max - 1/n) / (1 - 1/n)` for a choice, an ordered variant
  for a score, none for a noul. OpenAI: undocumented; its doc examples match Jev's formula
  (0.95 of 4 → 0.93, 0.7 of 3 → 0.55). Probabilities are the only uniform quantity.
- Refusals: OpenAI answers a single question with `type: "refusal"` while the others still answer.
  Gemini fails the whole request with HTTP 400 and `error.code` in `safety`,
  `prohibited_content`, `blocklist`, `image_safety`, `content_blocked`. Jev has none.
- A choice always spreads probability over the given options, even when none fits: callers need
  a catch-all option or a separate yes/no scope question.

## Usage

- OpenAI: `usage.input_tokens` **includes** `input_tokens_details.cached_tokens` and
  `cache_write_tokens` (subtract them for `ModelUsage`, whose categories are disjoint).
- Gemini: `total_input_tokens` + `input_tokens_by_modality` (text, image, audio; video frames and
  PDF pages count as image tokens).
- Jev: `input_tokens`, `output_tokens` (reported, free).

## Go clients

- OpenAI: `client.Decisions.New(ctx, openai.DecisionNewParams{...})` in
  `github.com/openai/openai-go/v3`, since v3.73.0 (2026-10-06); question `Instructions` became a
  typed union in a later minor (breaking). core pins v3.98.0 and does not use it.
- Gemini: `client.Decisions.Create` (`google.golang.org/genai/interactions`), committed to
  go-genai main on 2026-11-11 (4e1c734a) and in no release yet (latest tag v1.77.0, which core pins).
- TypeSafe: no Go SDK (Python and JavaScript only). OpenAPI at `https://api.typesafe.ai/openapi.json`.
  Bearer key. Retry 429 and 529 with backoff, honouring `retry-after`. Body ≤ 32 MiB.

## Docs (all serve markdown)

- OpenAI: `developers.openai.com/api/docs/guides/decisions.md`,
  `developers.openai.com/api/reference/resources/decisions.md`.
- Gemini: `ai.google.dev/gemini-api/docs/decisions-api.md.txt`, `ai.google.dev/api/decisions.md.txt`.
- TypeSafe: `docs.typesafe.ai/llms.txt` (index), `/primitives.md`, `/models.md`, `/api/errors.md`,
  `/confidence.md`.
