---
title: 'ai: the file store (image and document content outside messages)'
description: 'How ai_service keeps image and document bytes out of stored messages and decisions: the opaque `file` source, the host-provided FileStore (CreateFile, GetFileURL) set through a package variable, where content is moved and where files are resolved, and the rules a host implementation must follow (stable URLs, authorization).'
labels:
    repo: core
    tags: ai,files,images,documents,onyx
    topic: ai
---

An `Image` or `Document` block has three sources: inline `data`, a `url`, or a
`file`: the resource name of a file in the host's file store, opaque to core
(`malonaz/ai/v1/message.proto`). Messages and decision inputs are stored in
JSONB columns, and messages replayed on every turn, so inline bytes are copied
into the database and into every `ListMessages`. With a store, neither a
stored message nor a stored decision holds bytes.

## The store
`go/ai/ai_service/file_store.go`:

- `CreateFile(ctx, parent, mediaType, data) (file, error)`: stores content written under `parent`.
- `GetFileURL(ctx, parent, file) (url, error)`: the URL providers fetch `file` from.

`parent` is the parent of the chat or decision the content belongs to
(`organizations/{organization}/users/{user}`).

The host sets it with `ai_service.SetFileStore` **before the service is built**
(from the binary's `setup`). It is a package variable because onyx builds a
service from its manifest alone, and a host's store usually calls a service
that itself depends on ai-service: declared as a `grpc_client`, that is a
service cycle onyx rejects. The store is a request-time dependency only, never
part of start order or health.

Without a store nothing changes: `data` stays inline, and a `file` source fails
the generation with `FailedPrecondition`.

## Where it acts
- **Write** (`storeFiles`): `CreateMessage`, `UpdateMessage` when the mask covers
  `blocks`, and every generated block before it is streamed or accumulated
  (`generateMessageWrapper.Send`). `data` becomes `file`. Re-saving a message's
  `blocks` is therefore how old inline content is moved out.
- **Read** (`resolveFiles`): on the provider-bound history only, `file` becomes
  `url`. Providers are untouched: they already pass URLs through or download them.
  A file that cannot be resolved fails the generation and flags its input
  messages, like a provider refusal.
- **Decisions** (`CreateDecision`): the model is given a clone of the decision
  with its files resolved, and inline input is moved into the store only once
  the model has answered, so a failed decision still persists nothing.

## Rules for a host implementation
- **The URL must never change** for a given file: the history is replayed
  verbatim every turn (`lores/ai/replay-determinism`). A presigned URL minted
  per call breaks thinking-block binding and the prompt cache.
- **Authorize in `GetFileURL`**: a caller can name any file. Check it against
  `parent` (same tenant) before returning a URL a model will read.
- `CreateFile` knows the parent and nothing else: metadata a caller cares about
  (labels, titles) is set by that caller afterwards, on the file the block names.

## Which providers fetch a URL (measured 2026-11-24)
A resolved file reaches the provider as a URL that 302-redirects to presigned S3 (onikisu's
`download_url`; it answers HEAD with 501). Measured with real keys, 5 runs per cell, PNG, JPEG, PDF:

| Provider (core path) | Images by URL | PDFs by URL | Notes |
|---|---|---|---|
| anthropic | passed through, fetched, redirect OK | passed through, fetched | ~1-2s |
| openai (Responses) | `image_url`, redirect OK | `file_url`, redirect OK | |
| google, google-vertex | core downloads and inlines | core downloads and inlines | Go's client follows the redirect. Sent as `fileUri` instead, the Gemini API fetches fine but Vertex takes 20-120s and often fails `URL_REJECTED` on the redirect: keep inlining |
| xai (Chat Completions) | passed through, redirect OK | **fails**: no file parts on chat completions | |
| groq (Chat Completions) | passed through, **fails on the redirect** ("received status code: 302"); a direct presigned URL works | **fails**: no file parts | |
| cerebras (Chat Completions) | **fails**: "Remote image URLs are not supported; send images as data URIs" | **fails**: no file parts | |
| baseten, moonshot | not measured (no working key) | | |

So a provider that cannot fetch must be sent inline data, downloaded by core, as the google
provider already does.
