---
title: AIP codegen — Get
description: What the generated Get{Resource} does beyond AIP-131 — tombstones are returned, joins are resolved in the store, and how the name is parsed.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# Get

Generated for `Get{Singular}` (`rpc/get.go`, `postgres/get.go`).

- The name is parsed by the model's `Parse{Resource}Name`; wildcards are
  `InvalidArgument`, an unparseable name is `InvalidArgument` (not
  `NotFound`).
- **Soft-deleted resources are returned**, `delete_time` set. Callers that
  need "live only" must check `delete_time` themselves; `Update` does.
  `Undelete` clears it again.
- Joined fields (`field_opts.join` — ancestor, reference and query joins)
  are resolved in the same SELECT, so `Get` never issues a second query.
- Multi-pattern resources match unset pattern identifiers against `IS NULL`,
  so `organizations/o/notes/n` and `organizations/o/authors/a/notes/n` are
  distinct rows even with the same `note_id`.
- `Update` and soft `Delete` call `Get` internally to compute the new etag;
  a hand-written override of `Get` therefore affects both.
