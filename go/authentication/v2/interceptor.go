package authentication

import (
	"context"
	"errors"
	"fmt"
	"strings"

	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/internal/sessioncontext"
	coregrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	metadataKeyAuthorization = "authorization"
	wildcardScope            = "*"
)

// ErrSessionNotFound is returned by GetSession for a context no interceptor authenticated.
var ErrSessionNotFound = errors.New("session not found in context")

// GetSession returns the session of the call the context belongs to.
func GetSession(ctx context.Context) (*authenticationpb.Session, error) {
	session := sessioncontext.From(ctx)
	if session == nil {
		return nil, ErrSessionNotFound
	}
	return session, nil
}

// Authenticator is a proof core does not know: a webhook signature, a mesh identity.
type Authenticator interface {
	// Authenticate returns the session the call's credential proves, decided by its own grants, or nil when the
	// call carries no credential of its kind.
	Authenticate(ctx context.Context) (*authenticationpb.Session, error)
}

// Interceptor authenticates every call, decides whether it may proceed, and carries its session to the platform's
// other servers. One serves every server of a binary.
type Interceptor struct {
	authenticators             []Authenticator
	tokens                     *tokenAuthenticator
	apiKeys                    *apiKeyAuthenticator
	cookieName                 string
	serviceAccountIDToAccount  map[string]*serviceAccount
	roleIDToMethodToConditions map[string]methodToConditions
	methodToDescriptor         map[string]protoreflect.MethodDescriptor
	publicGrants               []*authenticationpb.Grant
	serviceToAccount           map[string]*serviceAccount
}

// NewInterceptor loads the configuration and compiles all of it: a fault in it fails here, never on a call.
func NewInterceptor(ctx context.Context, opts *Opts, authenticators ...Authenticator) (*Interceptor, error) {
	configuration, err := LoadConfiguration(opts.Config)
	if err != nil {
		return nil, err
	}
	interceptor := &Interceptor{
		authenticators:            authenticators,
		cookieName:                opts.CookieName,
		methodToDescriptor:        registeredMethods(),
		serviceAccountIDToAccount: map[string]*serviceAccount{},
		serviceToAccount:          map[string]*serviceAccount{},
	}
	if interceptor.roleIDToMethodToConditions, err = compileRoles(configuration.GetRoles(), interceptor.methodToDescriptor); err != nil {
		return nil, err
	}
	for _, roleID := range configuration.GetPublicRoles() {
		if _, ok := interceptor.roleIDToMethodToConditions[roleID]; !ok {
			return nil, fmt.Errorf("public role %q is not configured", roleID)
		}
		interceptor.publicGrants = append(interceptor.publicGrants, &authenticationpb.Grant{Role: roleID, Scope: wildcardScope})
	}
	serverKey, err := newSymmetricKey(opts.ServerSecret)
	if err != nil {
		return nil, fmt.Errorf("server secret: %w", err)
	}
	serverKeys := &keySet{set: jwk.NewSet()}
	if err := serverKeys.set.AddKey(serverKey); err != nil {
		return nil, fmt.Errorf("server secret: %w", err)
	}
	for _, config := range configuration.GetServiceAccounts() {
		account, err := interceptor.newServiceAccount(ctx, config, serverKeys, serverKey)
		if err != nil {
			return nil, err
		}
		interceptor.serviceAccountIDToAccount[config.GetId()] = account
	}
	claimsEnvironment, grantsEnvironment, err := newMappingEnvironments()
	if err != nil {
		return nil, err
	}
	interceptor.tokens = &tokenAuthenticator{}
	for _, config := range configuration.GetIssuers() {
		issuer, err := newIssuer(ctx, config, claimsEnvironment, grantsEnvironment)
		if err != nil {
			return nil, err
		}
		interceptor.tokens.issuers = append(interceptor.tokens.issuers, issuer)
	}
	if interceptor.apiKeys, err = newAPIKeyAuthenticator(ctx, opts.APIKeyHeader, opts.APIKeys, interceptor.serviceAccountIDToAccount); err != nil {
		return nil, err
	}
	return interceptor, nil
}

func (i *Interceptor) newServiceAccount(ctx context.Context, config *authenticationpb.ServiceAccount, serverKeys *keySet, serverKey jwk.Key) (*serviceAccount, error) {
	for _, grant := range config.GetGrants() {
		if _, ok := i.roleIDToMethodToConditions[grant.GetRole()]; !ok {
			return nil, fmt.Errorf("service account %q: unknown role %q", config.GetId(), grant.GetRole())
		}
		// No call's resources are derived yet, so a scope other than "*" could never be honoured.
		if grant.GetScope() != wildcardScope {
			return nil, fmt.Errorf("service account %q: scope %q of role %q: only %q is supported", config.GetId(), grant.GetScope(), grant.GetRole(), wildcardScope)
		}
	}
	account := &serviceAccount{config: config, subject: serviceAccountPrefix + config.GetId()}
	var err error
	switch keys := config.GetKeys().(type) {
	case *authenticationpb.ServiceAccount_Jwks:
		account.keys, err = newInlineKeySet(keys.Jwks)
	case *authenticationpb.ServiceAccount_JwksUri:
		account.keys, err = newRemoteKeySet(ctx, keys.JwksUri)
	default:
		account.keys, account.signingKey = serverKeys, serverKey
	}
	if err != nil {
		return nil, fmt.Errorf("service account %q: %w", config.GetId(), err)
	}
	return account, nil
}

// authenticatedCall is a call's session, and the grants of whoever presents it, which decide.
type authenticatedCall struct {
	session *authenticationpb.Session
	grants  []*authenticationpb.Grant
	// consumed is the metadata key of the credential used, removed before the handler runs.
	consumed string
}

// authenticate turns the call's one credential into a session. From outside, exactly one of a bearer token, an
// API key or another authenticator's proof; failing those, the cookie; failing that, an anonymous session. From
// another server, its token alone. Any other combination is UNAUTHENTICATED.
func (i *Interceptor) authenticate(ctx context.Context, fullMethod string) (*authenticatedCall, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	serverTokens := md.Get(metadataKeyServerToken)
	bearerTokens := bearerTokens(md)
	apiKeys := md.Get(i.apiKeys.header)
	var authenticatorSessions []*authenticationpb.Session
	for _, authenticator := range i.authenticators {
		session, err := authenticator.Authenticate(ctx)
		if err != nil {
			return nil, err
		}
		if session != nil {
			authenticatorSessions = append(authenticatorSessions, session)
		}
	}
	if credentials := len(serverTokens) + len(bearerTokens) + len(apiKeys) + len(authenticatorSessions); credentials > 1 {
		return nil, status.Errorf(codes.Unauthenticated, "a call carries one credential, this one carries %d", credentials).Err()
	}

	call := &authenticatedCall{}
	switch {
	case len(serverTokens) == 1:
		session, account, err := i.authenticateServer(ctx, serverTokens[0], fullMethod)
		if err != nil {
			return nil, err
		}
		call.session, call.grants, call.consumed = session, account.config.GetGrants(), metadataKeyServerToken
	case len(bearerTokens) == 1:
		session, err := i.tokens.authenticate(ctx, bearerTokens[0])
		if err != nil {
			return nil, err
		}
		call.session, call.grants, call.consumed = session, session.GetGrants(), metadataKeyAuthorization
	case len(apiKeys) == 1:
		account, err := i.apiKeys.authenticate(apiKeys[0])
		if err != nil {
			return nil, err
		}
		call.session, call.grants, call.consumed = proto.CloneOf(account.session()), account.config.GetGrants(), i.apiKeys.header
	case len(authenticatorSessions) == 1:
		// A copy: the authenticator may share its session, and the client is written into it.
		session := proto.CloneOf(authenticatorSessions[0])
		call.session, call.grants = session, session.GetGrants()
	default:
		session, err := i.authenticateCookie(ctx)
		if err != nil {
			return nil, err
		}
		call.session, call.grants = session, session.GetGrants()
	}
	for _, grant := range call.grants {
		if _, ok := i.roleIDToMethodToConditions[grant.GetRole()]; !ok {
			return nil, status.Errorf(codes.Internal, "grant of unknown role %q", grant.GetRole()).Err()
		}
	}
	if call.session.Client == nil {
		client, err := newClient(ctx, md)
		if err != nil {
			return nil, err
		}
		call.session.Client = client
	}
	// Logged before the decision, so a denial says who was denied.
	injectLogFields(ctx, call.session)
	return call, nil
}

// authenticateCookie reads the session cookie, through the gateway or grpc-web, as v1 did. An invalid token there is
// no credential: a page cannot clear an HttpOnly cookie, and rejecting it would lock the browser out of public methods.
func (i *Interceptor) authenticateCookie(ctx context.Context) (*authenticationpb.Session, error) {
	httpCookie, err := (&coregrpc.GatewayCookie{}).GetHTTPCookie(ctx, i.cookieName)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "reading gateway cookie: %v", err).Err()
	}
	if httpCookie == nil {
		httpCookie = coregrpc.WebCookie{}.GetHTTPCookie(ctx, i.cookieName)
	}
	if httpCookie == nil {
		return &authenticationpb.Session{}, nil
	}
	session, err := i.tokens.authenticate(ctx, httpCookie.GetValue())
	if status.HasCode(err, codes.Unauthenticated) {
		return &authenticationpb.Session{}, nil
	}
	return session, err
}

// bearerTokens returns the bearer tokens of the authorization headers. Another scheme is no credential of ours, as in v1.
func bearerTokens(md metadata.MD) []string {
	var tokens []string
	for _, value := range md.Get(metadataKeyAuthorization) {
		scheme, token, found := strings.Cut(value, " ")
		if found && strings.EqualFold(scheme, "bearer") && token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// authorize allows the call when a grant of the presenter, or a public role, allows the method over "*" and the
// condition of one of its rules holds. Nothing is allowed on an error.
func (i *Interceptor) authorize(fullMethod string, call *authenticatedCall, request any) error {
	var activation map[string]any
	var conditionErr error
	for _, grants := range [][]*authenticationpb.Grant{call.grants, i.publicGrants} {
		for _, grant := range grants {
			// No call's resources are derived yet: a scoped grant allows nothing.
			if grant.GetScope() != wildcardScope {
				continue
			}
			for _, condition := range i.roleIDToMethodToConditions[grant.GetRole()][fullMethod] {
				if condition == nil {
					return nil
				}
				if activation == nil {
					activation = conditionActivation(call.session, request)
				}
				allowed, err := evaluateCondition(condition, activation)
				if err != nil {
					conditionErr = err
					continue
				}
				if allowed {
					return nil
				}
			}
		}
	}
	if call.session.GetPrincipal() == nil && call.session.GetCaller() == nil {
		return status.Errorf(codes.Unauthenticated, "%s requires a credential", fullMethod).Err()
	}
	if conditionErr != nil {
		return status.Errorf(codes.Internal, "evaluating a condition on %s: %v", fullMethod, conditionErr).Err()
	}
	return status.Errorf(codes.PermissionDenied, "no grant allows %s", fullMethod).Err()
}

// emptyPrincipal stands for an absent principal, actor or caller: rules read an empty subject.
var emptyPrincipal = &authenticationpb.Principal{}

func conditionActivation(session *authenticationpb.Session, request any) map[string]any {
	activation := map[string]any{
		"principal": orEmpty(session.GetPrincipal()),
		"actor":     orEmpty(session.GetActor()),
		"caller":    orEmpty(session.GetCaller()),
	}
	if request != nil {
		activation["request"] = request
	}
	return activation
}

func orEmpty(principal *authenticationpb.Principal) *authenticationpb.Principal {
	if principal == nil {
		return emptyPrincipal
	}
	return principal
}

// handlerContext is the context the handler runs with: the session on it, the credential used off its metadata so
// that handlers forwarding headers never pass it on, and the account its outgoing calls are made as.
func (i *Interceptor) handlerContext(ctx context.Context, fullMethod string, call *authenticatedCall) context.Context {
	if call.consumed != "" {
		md, _ := metadata.FromIncomingContext(ctx)
		md = md.Copy()
		delete(md, call.consumed)
		ctx = metadata.NewIncomingContext(ctx, md)
	}
	ctx = sessioncontext.With(ctx, call.session)
	if account, ok := i.serviceToAccount[serviceName(fullMethod)]; ok {
		ctx = context.WithValue(ctx, servingKey{}, account)
	}
	return ctx
}

// Unary returns the unary server interceptor.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		call, err := i.authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		if err := i.authorize(info.FullMethod, call, request); err != nil {
			return nil, err
		}
		return handler(i.handlerContext(ctx, info.FullMethod, call), request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
}

// Stream returns the stream server interceptor. It authenticates before reading anything; conditions then read the
// stream's first request, peeked as v1 did, except on a bidirectional stream, where either side may send first.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		call, err := i.authenticate(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		peeked := &peekedServerStream{ServerStream: stream}
		var request proto.Message
		if !(info.IsClientStream && info.IsServerStream) {
			request = dynamicpb.NewMessage(i.methodToDescriptor[info.FullMethod].Input())
			if err := stream.RecvMsg(request); err != nil {
				return err
			}
			peeked.request = request
		}
		if err := i.authorize(info.FullMethod, call, request); err != nil {
			return err
		}
		peeked.ctx = i.handlerContext(stream.Context(), info.FullMethod, call)
		return handler(srv, peeked)
	}
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
}

// peekedServerStream gives the handler its context, and replays the request peeked for authorization to its first
// RecvMsg.
type peekedServerStream struct {
	grpc.ServerStream
	ctx     context.Context
	request proto.Message
}

func (s *peekedServerStream) Context() context.Context { return s.ctx }

func (s *peekedServerStream) RecvMsg(message any) error {
	if s.request == nil {
		return s.ServerStream.RecvMsg(message)
	}
	protoMessage := message.(proto.Message)
	proto.Reset(protoMessage)
	proto.Merge(protoMessage, s.request)
	s.request = nil
	return nil
}
