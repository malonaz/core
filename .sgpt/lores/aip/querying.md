---
title: Querying AIP-compliant APIs
description: 'Querying AIP-compliant APIs: AIP-160 filter syntax, unset-field (NULL) semantics of every operator, update masks, response read masks, Get/Batch over List.'
labels:
    lang: go
    repo: core
    topic: aip
---

All APIs are **Google AIP-compliant** (https://google.aip.dev). Key implications:

## List Endpoints (AIP-132 & AIP-160)
- Always set `page_size` (e.g. 100).
- Use the `filter` field for server-side filtering using **AIP-160 (CEL-like) syntax**:
  - Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
  - Logical: `AND`, `OR`, `NOT`
  - Traversal: dot notation for nested fields (e.g. `metadata.company_name`)
  - **Presence check**: use `:*` to check if a field is set (e.g. `metadata.status:*`)
  - **Map access**: use dot notation for map keys (e.g. `labels.mykey = "value"`). Use `:*` to check presence of a map key (e.g. `labels.mykey:*`).
  - **Non-standard map keys**: quote keys containing special characters (e.g. `labels."non-standard-key" = "hello"` or `labels."non-standard-key":*`).
  - **Wildcards**: string fields support leading and/or trailing `*` wildcards — `"*hello"`, `"hello*"`, `"*hello*"`. Wildcards are only supported as the first or last character (or both).
  - String literals are double-quoted: `metadata.status = "active"`
  - Enum values are **unquoted**: `type = CONTACT_CLASSIFICATION_SPAM`
  - Timestamps use RFC-3339: `create_time > "2025-01-01T00:00:00Z"` (UTC) or with a timezone offset like `"2025-01-01T00:00:00-05:00"` (UTC-5). The offset shifts the effective UTC comparison point — e.g. `00:00:00+05:00` equals `19:00:00Z` the previous day.
  - Examples:
    ```
    metadata.company_name = "Acme Corp"
    metadata.state = "CA" AND metadata.status != BOOK_STATUS_PUBLISHED
    create_time >= "2025-06-01T00:00:00Z" AND metadata.tags:*
    metadata.company_name = "*Corp"
    labels."my-custom-key":*
    ```
- Prefer a precise `filter` over fetching all results and filtering client-side.
- Use `order_by` when available (e.g. `create_time desc`).

### Unset fields (NULL semantics)

A filter evaluates against the resource **as the API presents it**, never
against how the row is stored. The storage detail that makes this matter:
the model codegen stores a `nullable` scalar's proto zero value as `NULL`
(`""`, `0`, `false`, `*_UNSPECIFIED` all become `NULL`), and protojson omits
zero-valued scalars, so a missing JSONB key is `NULL` too. The API renders
that `NULL` back as the zero value — so, exactly as AIP-160 requires, **an
unset scalar is its zero value** and every operator treats it that way.

| Field type | Unset means | `= zero` | `!= x` | `< x` / `> x` | `:*` (presence) |
|---|---|---|---|---|---|
| string, number, bool, enum | the zero value (`""`, `0`, `false`, `*_UNSPECIFIED`) | matches | matches | matches iff zero would (`count < 10` yes, `count > 0` no) | true iff **non-zero** (AIP-160: "present only if it has a non-default value") |
| timestamp, duration | absent — no value at all | never | **matches** (absent differs from everything) | never (absent orders with nothing) | true iff set |
| message | absent | — | see traversal below | — | true iff set |
| repeated, map | absent ≡ empty | — | — | — | true iff non-empty |

Consequences worth internalising:

- `state != STATE_X` **includes** rows where `state` was never set. To
  exclude them too: `state != STATE_X AND state:*`.
- `state = STATE_UNSPECIFIED` is the way to ask "never classified"; so is
  `NOT state:*`. Both match the same rows.
- `NOT expr` means "rows where `expr` is not true" — it always admits the
  rows a three-valued SQL comparison would drop. `NOT a = 1` ≡ `a != 1`.
- `count:*` is *not* "count is set", it is "count is non-zero"; `flag:*`
  ≡ `flag = true`. Use `>= 0` if you truly want every row.
- Wildcards never match an unset string (`title = "*x*"` needs content).
- Column-vs-column comparisons (`last_inbound_event_time >
  last_outbound_event_time`) are plain SQL: a NULL on either side is a
  non-match. Guard with `:*` if needed.

**Traversal** (`a.b != x`, AIP-160): if any *message* in the chain is
unset, the entry never matches — even on `!=`. `metadata.language != "en"`
matches `metadata = {}` (leaf unset ≡ `""`) but not a resource whose
`metadata` message is absent. **Maps are the documented exception**:
undefined keys behave like an unset scalar, so `labels.archived != "true"`
matches resources with no `archived` label *and* resources with no labels
at all — this is what "not archived" relies on everywhere.

Known leniencies (accepted, not wrong results): ordering operators are
accepted on enums, and a literal is tolerated on the left-hand side.

Implementation: `go/aip/transpiler/postgres/null.go`. The zero test is
decided at transpile time from the column type and the literal's proto
value, emitting `(col IS NULL OR col OP $n)` only when zero satisfies the
comparison — the common non-zero equality stays a plain, index-friendly
`col = $n`. `NOT` wraps three-valued operands in `COALESCE(…, FALSE)`.

## Update (AIP-134)
- Use `update_mask` for partial updates.
- The resource must always include its `name` field set to the full resource name — this identifies which resource to update.
- The `update_mask` specifies which fields to modify — only those fields are written; all others are left unchanged.
- Field paths use snake_case dot notation matching the proto schema (e.g. `metadata.status`, `display_name`).
- To clear a field, include it in the mask but leave it unset/empty in the resource.

## Other AIP Patterns
- **Get (AIP-131)**: Prefer Get over List when you already have the resource name.
- **Batch (AIP-231)**: Use batch methods (e.g. BatchGetLeads) when fetching multiple known resources.

## Response Read Mask
- When a tool schema includes a `response_read_mask` parameter, use it to control which fields are returned in the response.
- Specify comma-separated snake_case field paths (e.g. `name,metadata.status,create_time`).
- Use dot notation for nested fields (e.g. `metadata.company_name`).
- Paths are relative to the **resource**, not the response envelope — for List, and BatchGet RPCs, the mask applies to each resource in the list.
- Always set `response_read_mask` to only the fields you need to minimize response size and improve clarity.
- Use `*` to return all fields only if necessary.
