---
title: Querying AIP-compliant APIs
description: 'Querying AIP-compliant APIs: AIP-160 filter syntax, update masks, response read masks, Get/Batch over List.'
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
