---
title: 'authentication: delegated sessions'
description: 'Delegated sessions in go/authentication: a service acting as a principal on behalf of an actor (agents), SessionManager.Delegate, the never-authorized invariant, expire_time, actor in CEL, delegation_authorization_cel, act_as_cel + principal_field, the JWT act claim, and how the agent service adopts it.'
labels:
    repo: core
    topic: authentication
---

- **Delegate, never sign**: `SessionManager.Delegate(ctx, principal, actor, lifetime)` is the only public way to
  mint a session. It sets identity = principal, `delegation.actor`, `expire_time`, `authorized = false`, and
  *replaces* the outgoing metadata, so the caller's own credentials never ride along. Only the permission
  interceptor's own check flips `authorized`; later internal hops then trust it like any edge-checked session.
- **Expiry** is checked by `SessionManager.verify` on every hop: a call must finish before its session expires (the
  agent service sets the call deadline = expiry).
- **CEL**: issuer `method_to_authorization_cel` gets `actor` ("" unless delegated) and `session`;
  `PermissionConfiguration.delegation_authorization_cel` (`actor`, `method`, `session`) restricts every delegated
  session on non-public methods, whatever its identity. A delegated service-account principal gets its configured
  labels at the edge check.
- **act-as**: a method declaring `option (malonaz.authentication.v1.principal_field) = "a.b"` has the interceptor
  check, even on authorized sessions, that the caller may act as the `Identity` named there: only service accounts
  can be named (itself, or callers its `act_as_cel` over `session` admits); JWT identities are only ever copied from
  the caller's session (`authentication.IdentityOf`).
- **JWT seam**: `JwtMinter.MintDelegatedJwt` puts the actor in RFC 8693 `act.sub`; `JwtAuthenticationInterceptor`
  maps a raw `act.sub` back to `Session.delegation`.
- **Agent service**: `Agent.principal` defaults to the creator's identity, sub-agents inherit their parent's, and
  tool calls run on `Service.delegate`. Internal code needing the principal reads with `s.getAgent` (unredacted);
  every RPC returning an Agent redacts it to issuer + `sub` (or the service account) via `redacted`/`redactAgent`,
  and StreamAgent redacts snapshots in `send`.
- JWT principals reach only methods their issuer's CEL lists (hosts like onikisu list gateway methods only), so
  agents of users get gateway tools; internal methods need a service-account principal.
