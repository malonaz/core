---
title: Authentication v2 — the session, the decision, the server token
description: 'go/authentication/v2 and malonaz.authentication.v2: one interceptor per binary, the Session (principal, actor, caller, grants, client), the configuration (issuers, service accounts, roles, public roles), the decision over grants, every edge outcome, the token one server presents to another, how a host adopts it, why v1 still exists, what is not built yet.'
labels:
    lang: go, protobuf
    repo: core
    topic: authentication, authorization, grpc
---

# Authentication v2

`go/authentication/v2` (package `authentication`) with the proto
`malonaz/authentication/v2`. It sits **beside** v1 (`go/authentication`,
`malonaz.authentication.v1`), which is unchanged: onikisu still runs v1 and
moves in one switch; v1 is deleted once nothing uses it. v2 never imports v1.
Core's own `cmd/tsunade` runs v2.

## The session

Every call that passes the interceptor carries one `Session`, read by
`authentication.GetSession(ctx)`:

| Field | What it is |
|---|---|
| `principal` | who the call is made as: `{subject, attributes}`; unset when anonymous |
| `actor` | who makes it for the principal (an operator logged in as a customer); rights stay the principal's |
| `caller` | the server presenting the call, when one server calls another; unset from outside |
| `grants` | what the principal's credential grants: `{role, scope}` |
| `client` | IP, user agent, platform, version, time zone of the original caller |

A service account's subject is `serviceAccounts/{id}`.

## Configuration

One jsonnet file (`--authentication.config`), parsed strictly and validated
(protovalidate), then **compiled entirely at startup**: a fault fails the
server there, never a call.

- `roles`: `{id, rules: [{methods, condition_cel}], includes}`. `methods` are
  full method names; `*` matches any run of characters (v1's construction);
  every pattern must match a registered method.
- `issuers`: matched by `iss` **and** `aud`; keys inline (`jwks`, every key
  naming its `alg`, tried whatever the token's `kid`) or by `jwks_uri` (fetched
  at startup, `kid` required). Four CEL mappings over `claims`:
  `subject_cel` (default `claims.sub`), `attributes_cel` (default the claims),
  `actor_cel` (default `act.sub`), `grants_cel` (required; builds
  `[Grant{role: ..., scope: ...}]`, short names via the
  `malonaz.authentication.v2` container).
- `service_accounts`: grants, attributes, optional own keys (else the
  platform's `--server-secret`).
- `public_roles`: held by every session, anonymous ones included.

Conditions read `principal`, `actor`, `caller` (each an empty `Principal`
when absent, so `.subject` is `""`) and `request`, typed per method; `request`
is not declared on bidirectional streams. The `rn.*` functions are available.
A condition not reading `request` compiles once for all methods.

## The decision

Allowed when, for some grant **of the presenter** (or a public role), a rule
of its role or an included role lists the method, the scope is `*`, and the
rule's condition, if any, holds. The presenter is the principal from outside,
the **caller** (its configured grants) between servers. Between servers,
onikisu reproduces v1's free carrying with one role every internal account
holds: `methods: ['*']`, `caller.subject != "" && principal.subject !=
caller.subject` (true for a carried anonymous call; false for a service
calling as itself, which its own grants decide).

## Credentials and outcomes

A call carries exactly one explicit credential: `authorization: Bearer`, the
API key header (`--api-key-header`, default `x-api-key`; keys from
`--api-keys` as `id:key` pairs, several per account), a server's token
(`x-server-token`), or an extra `Authenticator`'s proof. Without one, the
cookie (`--cookie-name`, gateway or grpc-web); without that, anonymous. The
credential used is removed from the incoming metadata (the cookie, as in v1,
stays). A stream is authenticated before its first message is read. The tests in
`go/authentication/v2/edge_test.go` are named after these rows.

| Situation | Outcome |
|---|---|
| token malformed, unknown `iss`+`aud`, bad signature, expired, algorithm its key does not name, no `exp`, matching two issuers | UNAUTHENTICATED |
| invalid token in the cookie | ignored: anonymous |
| cookie beside an explicit credential or a server's token | the other credential is used |
| two explicit credentials, or two of one kind | UNAUTHENTICATED |
| a mapping fails, or the subject is empty | UNAUTHENTICATED |
| a deciding grant names an unknown role | INTERNAL |
| server token: bad signature, expired, other `aud`, unknown account | UNAUTHENTICATED |
| grant scope other than `*` | allows nothing (no resource derivation yet) |
| nothing allows | PERMISSION_DENIED; UNAUTHENTICATED when anonymous |
| a condition errors and nothing allows | INTERNAL (UNAUTHENTICATED when anonymous) |
| unknown method/role/include, include cycle, non-bool condition, `request` on a bidi stream, duplicate id, service account scope other than `*`, stale public method | the server refuses to start |
| API key of an account the configuration lacks | skipped with a warning naming the account: that key proves nothing |
| key set URL unreachable during a call | INTERNAL |

Health checks skip the interceptor. Hooks and the other POST interceptors of
`go/grpc/server.go` run after it, only for calls it let in.

## Between servers

`authentication.Credentials()` is a per-RPC credential, installed only on
connections to the binary's own servers. Per call it mints an HS256 JWT under
`--server-secret`: `iss` = `serviceAccounts/{calling account}`, `sub` the
principal (absent when anonymous), `act` `{sub: actor}`, `aud` the destination
gRPC service, `exp` one minute, and `session` = base64url of the binary
`Session` (principal, actor, grants, client; never the caller). What the token
carries is taken as written: only a holder of the key signs one. The calling
account is `As(account)`'s, else the account `Serve(account, services...)`
named for the gRPC service handling the current call. A context with a
session but no serving account fails the outgoing call (INTERNAL); one with
neither calls anonymously. Cost: about 21 µs to sign and 39 µs to verify and
decide (`plz run //go/authentication/v2:benchmark -- -test.bench .`).

## Adopting it

- onyx: `interceptors: [INTERCEPTOR_AUTHENTICATION]` (`lores/onyx/binary`);
  flags under `authentication` (`AUTHENTICATION_CONFIG`, `_API_KEYS`,
  `_API_KEY_HEADER`, `_COOKIE_NAME`, `_SERVER_SECRET`). Every gRPC server of
  such a binary must list it (generation fails otherwise).
- Every onyx service needs a service account of its name (onyx calls `As`
  and `Serve` with it).
- The client IP is X-Forwarded-For's leftmost entry, as v1 read it: the
  caller controls it.
- v1's rule texts move unchanged: wrap each as
  `cel.bind(claims, principal.attributes, cel.bind(subject, principal.subject, (<v1 text>)))`
  (conditions have CEL's bindings extension). It evaluates as v1 did
  (`TestEquivalence_Conditions`), but for `subject` when an issuer's rewrite
  drops `sub`: v1 read `""`, v2 reads the issuer's subject.
- Issuing tokens: `NewSigner(issuer, authentication.SymmetricKey(secret))`,
  `Sign(Token{Subject, Actor, Lifetime, Claims})`; writes the claims v1's
  minter wrote, no `kid`.
- Tests: `authenticationtest.NewContext(ctx, session)`, `KeySet`,
  `NewIssuer`. Give the configuration a rule test: every caller against
  every method (`cmd/tsunade/authentication_test.go`).

## Not built yet

Scopes other than `*` (needs the derivation of the resources a call names,
and the `resources` condition variable); `ExchangeToken`; keys per service
account that this process can sign with (an account with its own keys can be
verified, not `As`'d or `Serve`'d); clock skew between pods (tokens never
leave the process today).
