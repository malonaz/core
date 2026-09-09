---
title: AIP codegen — Update
description: What the generated Update{Resource} does beyond AIP-134 — the mandatory update option and its path allow-list, read-patch-write with etag retry, the CEL precondition, and what can never be updated.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# Update

Generated for `Update{Singular}` (`rpc/update.go`, `postgres/update.go`).

## The `update` option is mandatory

```proto
message UpdateBookRequest {
  option (malonaz.codegen.aip.v1.update) = {paths: ["title", "metadata", "labels"]};
  ...
}
```

`paths` is the allow-list for `update_mask`: a listed path authorizes itself
and every descendant (`metadata` covers `metadata.foo`, `labels` covers
``labels.`my key` ``). A mask path outside the list, or the empty mask, is
`InvalidArgument`. Always forbidden regardless of the list: `name`,
`create_time`, `delete_time`, and any `IDENTIFIER`/`IMMUTABLE`/
`OUTPUT_ONLY` field. `update_time` and `etag` are stamped server-side and
need not be listed.

## Read–patch–write

1. `Get` the resource (a tombstone is `NotFound`).
2. `proto.Clone`, apply the mask (`pbfieldmask` semantics: sub-paths into
   `as_json_bytes` messages patch inside the JSONB column; a map key path
   sets/clears just that key), run `protovalidate` on the **patched**
   resource → `InvalidArgument`.
3. Evaluate `precondition` (below).
4. Stamp `update_time`, recompute `etag`, `UPDATE … SET <only the touched
   columns> WHERE ids AND etag = $old`.

## Etag and the retry loop

- Client supplied `etag` and it no longer matches → `Aborted`, no retry.
- Client supplied none → the server uses the etag it just read and, on
  `Aborted`, **retries the whole read-patch-write until success or context
  cancellation**. This is what makes two concurrent masked updates to
  different keys of the same JSONB column both land instead of the last
  writer clobbering the first.

## Precondition (CEL)

Opt in with `string precondition` on the request. Compiled per call against
two variables, `previous_<resource>` and `<resource>` (the patched copy);
must type-check to `bool` (`InvalidArgument` otherwise); `false` is
`FailedPrecondition`. Example: `previous_book.state == "DRAFT"`.

## Events

With `event.updated` configured, the event carries the new resource, the
previous one and the mask; the optional CEL gate sees `update_mask` too.
