---
title: Authentication v2 — the session, the decision, the server token
description: 'go/authentication/v2 and malonaz.authentication.v2: one interceptor per binary, the Session (principal, actor, caller, grants, client), the configuration (issuers, service accounts, roles, public roles), the decision over grants, scopes (the resources a call names by its AIP shape, segment covering, failing closed), every edge outcome, what each call logs, the token one server presents to another, ExchangeToken (a token of a host issuer for a subject, made by a named actor: the host''s Subjects, the exchange lifetime, no chains, the mapping check), how a host adopts it, what is not built yet.'
labels:
    lang: go, protobuf
    repo: core
    topic: authentication, authorization, grpc
---

# Authentication v2

`go/authentication/v2` (package `authentication`) with the proto
`malonaz/authentication/v2`: core's only authentication, run by `cmd/tsunade`
and onikisu. It replaced v1 (`go/authentication`, `malonaz.authentication.v1`,
deleted), keeping its tokens, headers and rule texts; the path keeps its `v2`.

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
of its role or an included role lists the method, the grant's scope covers
every resource the call names (`*` covers everything, whatever the method),
and the rule's condition, if any, holds. The presenter is the principal from outside,
the **caller** (its configured grants) between servers. Between servers,
onikisu reproduces v1's free carrying with one role every internal account
holds: `methods: ['*']`, `caller.subject != "" && principal.subject !=
caller.subject` (true for a carried anonymous call; false for a service
calling as itself, which its own grants decide).

## Scopes

A scoped grant (`organizations/1`) covers the resources the call names,
read from the request by the method's AIP shape (`resources.go`, derived once
per registered method at startup; the verb is the method name's first word,
so `Listen` is no `List`):

| Method | Resources |
|---|---|
| `Create*`, `List*`, `Search*`, `Stream*` | `parent` |
| `Update*` | the `name` of the request's one field typed as a resource (`google.api.resource`) |
| `BatchGet*` | every `names`, and `parent` when set |
| `BatchCreate*` | every request's `parent` (the batch's when unset), and the batch's when set |
| any other, `Get*`/`Delete*`/custom included | `name`; with no `name` field, `parent` (a collection's custom method, AIP-136: `Import*`, `Export*`, `Compute*Stats`) |

- Fails closed: a request lacking the shape's field (a `Create` with no
  `parent`, a custom method with neither `name` nor `parent`), and every client or bidi stream (only the first message is
  read), names nothing derivable: a scoped grant never allows it. A service
  account whose scoped grant's role lists such a method refuses to start; an
  issuer's grants are only known per call.
- Covering is a literal prefix on segments: the name is the scope or starts
  with `scope + "/"`. `organizations/1` covers `organizations/1/shelves/-`, never
  `organizations/12` nor `organizations/-/...`; a scope holding `-`, or empty,
  covers nothing (a service account's refuses to start). Never `rn.hasParent`,
  which reads `-` in either argument as anything.
- An empty value is a resource no scope covers; a call naming none is denied.
- Batches check more than the AIP contract strictly needs (names beside the
  parent): a hand-written handler may not hold names to their parent.
- The decision reads the request the handler will run: a host's request
  rewrites (`middleware.WithHookRewrite`, e.g. legacy names into current ones)
  run before the interceptor, a stream's first request included. A rewrite runs
  for every call, refused ones too, before any session exists: it only rewrites.
  An ordinary hook rewriting the request runs after the decision: unchecked.

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
| scoped grant, call names no resource (or none derivable) | that grant does not allow |
| nothing allows | PERMISSION_DENIED; UNAUTHENTICATED when anonymous |
| a condition errors and nothing allows | INTERNAL (UNAUTHENTICATED when anonymous) |
| unknown method/role/include, include cycle, non-bool condition, `request` on a bidi stream, duplicate id, service account scoped grant on a method whose resources are not derived or with a wildcard scope, stale public method | the server refuses to start |
| API key of an account the configuration lacks | skipped with a warning naming the account: that key proves nothing |
| key set URL unreachable during a call | INTERNAL |

Health checks skip the interceptor. Rewrite hooks run before it, for every
call; the other hooks (checks reading the session) and the other POST
interceptors of `go/grpc/server.go` (canonicalize, validate, field masks) run
after it, only for calls it let in: a refused call sees UNAUTHENTICATED or
PERMISSION_DENIED, never a validation error.

## The log

Each field present is added to the call's log line (`log.go`), once
authenticated and before the decision, so that a denial says who was denied:
`principal`, `issuer` (the configured issuer's id, for a token, bearer or
cookie), `actor`, `caller`, `attribute.{key}`, `ip_address`,
`client_platform`, `client_version`, `user_agent`. An allowed call adds
`role`: the role held (a grant's or a public role, never an included one)
whose rule allowed it. A carried call logs no `issuer`: no session carries it,
the edge's line has it.

## Between servers

Identity rides the call's context, as in v1. The interceptor puts on each
handler's context, and `As` on a service's, what core's gRPC client asks for
on every outgoing call (`grpc.WithCallMetadata`, read by a per-RPC credential
every `grpc.Connection` has): nothing for a host, the generator or a manifest
to declare, and any connection made with `grpc.NewConnection` carries it. As
the call is sent (once a transport is ready, once per attempt) it mints an
HS256 JWT under `--server-secret`: `iss` = `serviceAccounts/{calling account}`,
`sub` the principal (absent when anonymous), `act` `{sub: actor}`, `aud` the
destination gRPC service, `exp` one minute, and `session` = base64url of the
binary `Session` (principal, actor, grants, client; never the caller). What the
token carries is taken as written: only a holder of the key signs one. The
calling account is `As(account)`'s (it wins over a handler's session), else
the account `Serve(account, services...)` named for the gRPC service handling
the current call. A context with a session but no serving account fails the
outgoing call (INTERNAL); a context with neither sends nothing. A context kept
past its call (`context.WithoutCancel`, a goroutine) signs fresh tokens. Cost:
about 21 µs to sign and 39 µs to verify and decide (`plz run
//go/authentication/v2:benchmark -- -test.bench .`).

- A remote server called on such a context receives the token too, addressed
  to its service and dead in a minute. A remote server on v2 under another
  secret refuses it (UNAUTHENTICATED), and one shown an API key as well sees two
  credentials: call it on a context carrying no identity.
- To strip identity from a context that carries some (a handler's, a service's
  `As`), replace its call metadata with a function returning none
  (`grpc.WithCallMetadata`); clearing the outgoing gRPC metadata strips nothing,
  as the token is minted per call. To call with another credential, replace it
  with a function returning that credential alone: the AgentService calls its tool
  targets with a token exchanged for the agent's owner so (`asOwner` in
  `go/ai/agent_service`).
- Who presented a call is `caller`, which only a holder of the server secret
  can set: a handler only another server may call checks
  `caller.subject` (`serviceAccounts/{id}`), never a header or the principal.
  The AgentService tells a call forwarded by another replica by its own
  account as caller, and runs WakeAgent only for the scheduler dispatcher's.
- The grpc-gateway and the grpc-web proxy relay only the outside caller's
  credential: their connections are not core's, and their requests' contexts
  carry no identity (`TestEdgeRelaysOnlyTheOutsideCredential`).
- A connection made with gRPC directly (`grpc.NewClient`) carries nothing;
  `grpcinproc` and pbreflection's in-process client call the handler with the
  caller's context, so it reads the caller's session and passes no interceptor.

## Exchange

`TokenService.ExchangeToken` (`token_service.proto`, `exchange.go`) lets a service call the
platform as a user while they are away: it issues a token of one of the host's issuers with
`sub` = `request.subject`, `act.sub` = `request.actor`, and the claims the host gives for the
subject **now**. The token is then presented as `authorization: Bearer`, like any client's, and
the issuer's mappings turn it into the session: the subject's own grants decide every call (never
the exchanging service's).

- Who exchanges is an ordinary rule on the method; conditions may read `request.issuer`,
  `request.subject`, `request.actor`. The request names no resource: only a grant over `*`
  allows it.
- An issuer is exchanged for when it sets `exchange_lifetime`: the lifetime of its exchanged
  tokens and the most a request may ask for (a longer one is capped; none gets it).
- The host answers claims through `Subjects.Claims(ctx, issuer, subject)`; its error refuses with
  its code (NotFound for a subject it does not know). Core never knows where they come from.
  Claims naming one the signer writes (`iss`, `aud`, `sub`, `act`, `iat`, `nbf`, `exp`):
  INTERNAL.
- Before signing, the token is mapped through the issuer's expressions: unless they give exactly
  the requested subject and actor, INTERNAL. An issuer whose `actor_cel` ignores `act` (kenji's
  reads an `impersonator` claim) or whose `subject_cel` does not yield the request's subject
  cannot be exchanged for as is.
- Only a service calling as itself (`As`: caller present, principal = caller, no actor) exchanges; a carried
  session (anonymous included), an outside credential or an actor (every exchanged token has one: no chains) is
  PERMISSION_DENIED, whatever the rules allow (`carrier` included). A missing session (no interceptor): INTERNAL.
- Keys: `NewTokenService(ctx, configuration, subjects, issuerIDToKey)`, each key a JWK as
  `NewSigner` takes; exactly the issuers with `exchange_lifetime` get one, or it fails.
- Hosting it: onyx registers a service's `Service`, built from its manifest's dependencies, and
  has no kind for a host's Go value. The host writes a small onyx service of its own (Opts
  naming the configuration and the keys, a runtime embedding `*authentication.TokenService`,
  its `Subjects`) and serves it with `proto` the authentication v2 library; like every onyx
  service it needs a service account of its name, with no grants (the handler calls no one). Service accounts are not exchanged for: their
  grants live in the configuration, which no issuer's `grants_cel` reads.

## Adopting it

- onyx: `interceptors: [INTERCEPTOR_AUTHENTICATION]` (`lores/onyx/binary`);
  flags under `authentication` (`AUTHENTICATION_CONFIG`, `_API_KEYS`,
  `_API_KEY_HEADER`, `_COOKIE_NAME`, `_SERVER_SECRET`). Every gRPC server of
  such a binary must list it (generation fails otherwise).
- Every onyx service needs a service account of its name (onyx calls `As`
  and `Serve` with it). The scheduler dispatcher calls its endpoints, and
  ai-engine its reflection server, as their own accounts or carrying the
  caller's session: their grants (or `carrier`) must allow
  `/grpc.reflection.v1.ServerReflection/ServerReflectionInfo` and, for the
  dispatcher, the scheduler-run methods it delivers.
- The client IP is X-Forwarded-For's leftmost entry, as v1 read it: the
  caller controls it.
- v1's rule texts run unchanged: wrap each as
  `cel.bind(claims, principal.attributes, cel.bind(subject, principal.subject, (<v1 text>)))`
  (conditions have CEL's bindings extension; `TestRuleConditions`). They
  evaluated as under v1 (proven side by side before v1's deletion), but for
  `subject` when an issuer's rewrite drops `sub`: v1 read `""`, v2 reads the
  issuer's subject.
- Issuing tokens: `NewSigner(issuer, authentication.SymmetricKey(secret))`,
  `Sign(Token{Subject, Actor, Lifetime, Claims})`; writes `iss`, `aud` (a
  list), `sub`, `act`, `iat`, `nbf`, `exp` and the claims, no `kid`: the
  tokens v1's minter wrote (`TestSigner` pins them).
- Tests: `authenticationtest.NewContext(ctx, session)`, `KeySet`,
  `NewIssuer`. Give the configuration a rule test: every caller against
  every method (`cmd/tsunade/authentication_test.go`).

## Not built yet

A declaration of resources for methods outside the standard shapes (open:
a request message option was recommended, not added); the `resources` condition
variable; narrowing an exchange to fewer grants than the subject holds; keys per service
account that this process can sign with (an account with its own keys can be
verified, not `As`'d or `Serve`'d); clock skew. Signer and verifier share a
process today; once they are different machines, verification needs
`jwt.WithAcceptableSkew` of a few seconds: jwx v2.1.6 checks `exp`, `iat` and
`nbf` with no skew, so a token whose `iat` is a second ahead of the verifier's
clock is refused (`"iat" not satisfied`, UNAUTHENTICATED).
