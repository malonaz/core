# Authentication

## Session
The session object is always accompanied by a signature.
Anything inspecting the session always verifies the signature to ensure that it's an authentic session object.

## External API Key Authentication Interceptor
Looks for an 'api' key and creates a corresponding session.

## Internal Authenticator
Verifies that a session exists and that is signed by us.

## Session Injector
Copies the session from incoming metadata to a local context value.
Also injects some log fields from the session into the ctx for logging purposes.


## Interceptor Flow
1. Request comes in.
2. SessionManager.LocalContextInjectorInterceptor grabs an IncomingContext.SignedSession => sets it to local context (if it exists).
3. ExternalApiKeyAuthentication + (any other authentication interceptor) => can create a signed session and inject it into local context.
4. PermissionInterceptor => inspects local context and *must* see a signed session. Verifies the signed session!
   If session is authorized => let it through
   If session is not authorized => verify permissions => update session + resign => update in local context.
5. SessionManager.OutgoingContextInjectorInterceptor => grabs the local session and sets it on outgoing context. Perhaps we should be doing this on a client interceptor?
6. Request is processed by handler.

## Delegated Sessions
A service acts for a principal (a JWT or service account identity) at an actor's initiative (e.g. an agent) with
`SessionManager.Delegate`: the outgoing call carries only a signed session with `delegation.actor` set, an
`expire_time`, and `authorized=false`, so the service it reaches checks the principal's permissions plus the
restrictions on actors (`actor` and `session` in issuer CEL, `delegation_authorization_cel`). A service account
principal gets its configured labels at that check. Expired sessions are rejected on every hop.

Methods whose request names an `Identity` a resource will act as declare `(malonaz.authentication.v1.principal_field)`:
the permission interceptor then checks, even on authorized sessions, that the caller may act as it (a service
account's `act_as_cel`). JWT identities are never named, only copied from the caller's own session.

Outside the shared-secret domain, `JwtMinter.MintDelegatedJwt` mints a token with `sub` = the principal and RFC 8693
`act.sub` = the actor, which `JwtAuthenticationInterceptor` maps back to `Session.delegation`.
