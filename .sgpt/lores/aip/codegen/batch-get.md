---
title: AIP codegen — BatchGet
description: What the generated BatchGet{Plural} does beyond AIP-231 — all-or-nothing NotFound, order preservation, tombstones included, and the optional parent constraint.
labels:
    lang: go, protobuf
    repo: core
    topic: aip, codegen
---

# BatchGet

Generated for `BatchGet{Plural}` (`rpc/batch_get.go`, `postgres/batch_get.go`).

- **All or nothing**: one `SELECT … WHERE (ids) OR (ids) …`; if fewer rows
  come back than names were asked for, the whole call is `NotFound`
  (`expected N, found M`). Duplicated names therefore fail too.
- The response preserves request order (rows are re-keyed by `name`).
- **Tombstones are included** — like `Get`, unlike `List`.
- `parent` is optional. When set, every name must have it as a prefix
  (`InvalidArgument` otherwise); for multi-pattern resources the parent must
  additionally resolve to exactly one pattern and each name must match that
  pattern directly — a prefix check alone would accept deeper patterns.
- Wildcards in names are `InvalidArgument`; in `parent` they are allowed.
