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
Also injects some log fields from the session into the ctx for logging purposes.s


## Interceptor Flow
1. Request comes in.
2. SessionManager.PreInterceptor grabs an IncomingContext.SignedSession => sets it to local context (if it exists).
3. ExternalApiKeyAuthentication + (any other authentication) => can create a signed session and inject it into local context.
4. InternalAuthentication => inspects local context and *must* see a signed session. Verifies the signed session!
   If session is authorized => let it through
   If session is not authorized => verify permissions => update session + resign => update in local context.
5. SessionManager.PostInterceptor => Sets it on outgoing context.
6. Request is processed by handler.
