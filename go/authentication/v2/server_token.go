package authentication

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	coregrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const (
	// metadataKeyServerToken carries the token one server of the platform presents to another.
	metadataKeyServerToken = "x-server-token"
	// sessionClaim holds the rest of the session in a server's token, principal and actor attributes, grants and
	// client: the Session message, binary and base64url-encoded, as JSON costs several times more to sign and verify.
	sessionClaim = "session"
	// serverTokenLifetime is long enough for one call to be sent, short enough to be useless when replayed.
	serverTokenLifetime  = time.Minute
	serviceAccountPrefix = "serviceAccounts/"
)

// serviceAccount is a configured service account, with the keys its tokens verify with.
type serviceAccount struct {
	config  *authenticationpb.ServiceAccount
	subject string
	keys    *keySet
	// signingKey is the platform's key, nil for an account with keys of its own, which this process cannot sign as.
	signingKey jwk.Key
}

// principal is the account as a principal.
func (a *serviceAccount) principal() *authenticationpb.Principal {
	return &authenticationpb.Principal{Subject: a.subject, Attributes: a.config.GetAttributes()}
}

// session is the account calling as itself.
func (a *serviceAccount) session() *authenticationpb.Session {
	return &authenticationpb.Session{Principal: a.principal(), Grants: a.config.GetGrants()}
}

// As returns what makes calls on a context be made by the service account as itself: what onyx hands to a
// service's Start. It replaces any session the context carries.
func (i *Interceptor) As(account string) (func(context.Context) context.Context, error) {
	serviceAccount, err := i.signingAccount(account)
	if err != nil {
		return nil, err
	}
	callMetadata := carrier{account: serviceAccount, session: serviceAccount.session()}.metadata
	return func(ctx context.Context) context.Context {
		return coregrpc.WithCallMetadata(ctx, callMetadata)
	}, nil
}

// Serve names the service account serving gRPC services: calls its handlers make carry their session as it.
// Call it before serving.
func (i *Interceptor) Serve(account string, services ...string) error {
	serviceAccount, err := i.signingAccount(account)
	if err != nil {
		return err
	}
	for _, service := range services {
		i.serviceToAccount[service] = serviceAccount
	}
	return nil
}

func (i *Interceptor) signingAccount(account string) (*serviceAccount, error) {
	serviceAccount, ok := i.serviceAccountIDToAccount[account]
	if !ok {
		return nil, fmt.Errorf("unknown service account %q", account)
	}
	if serviceAccount.signingKey == nil {
		return nil, fmt.Errorf("service account %q has keys of its own: this process cannot sign as it", account)
	}
	return serviceAccount, nil
}

// carrier presents a session on the calls made on a context, as an account: the one As named, with itself as the
// principal, or the one serving the call the context belongs to, with that call's session.
type carrier struct {
	account *serviceAccount
	session *authenticationpb.Session
}

// metadata mints a call's token as the call is sent, so a context outliving its call still signs live tokens.
func (c carrier) metadata(ctx context.Context) (map[string]string, error) {
	requestInfo, _ := credentials.RequestInfoFromContext(ctx)
	if c.account == nil {
		return nil, status.Errorf(codes.Internal, "no service account serves this call: cannot carry its session to %s", requestInfo.Method).Err()
	}
	token, err := c.account.sign(c.session, serviceName(requestInfo.Method))
	if err != nil {
		return nil, err
	}
	return map[string]string{metadataKeyServerToken: token}, nil
}

// sign mints the token carrying a session to a service: `iss` the account, `sub` the principal, `act` the actor,
// `aud` the service, and the rest of the session in the `session` claim. The caller is never carried: the next
// server's caller is this account.
func (a *serviceAccount) sign(session *authenticationpb.Session, audience string) (string, error) {
	carried := &authenticationpb.Session{
		Principal: session.GetPrincipal(),
		Actor:     session.GetActor(),
		Grants:    session.GetGrants(),
		Client:    session.GetClient(),
	}
	sessionBytes, err := pbutil.Marshal(carried)
	if err != nil {
		return "", status.Errorf(codes.Internal, "marshaling session: %v", err).Err()
	}
	now := time.Now()
	builder := jwt.NewBuilder().
		Issuer(a.subject).
		Audience([]string{audience}).
		IssuedAt(now).
		Expiration(now.Add(serverTokenLifetime)).
		Claim(sessionClaim, base64.RawURLEncoding.EncodeToString(sessionBytes))
	if subject := session.GetPrincipal().GetSubject(); subject != "" {
		builder = builder.Subject(subject)
	}
	if actor := session.GetActor().GetSubject(); actor != "" {
		builder = builder.Claim(actorClaim, map[string]any{"sub": actor})
	}
	token, err := builder.Build()
	if err != nil {
		return "", status.Errorf(codes.Internal, "building token: %v", err).Err()
	}
	signed, err := signToken(token, a.signingKey)
	if err != nil {
		return "", status.Errorf(codes.Internal, "%v", err).Err()
	}
	return signed, nil
}

// authenticateServer verifies a server's token: signed by a configured service account, unexpired, and addressed
// to the service called. Anything else is UNAUTHENTICATED.
func (i *Interceptor) authenticateServer(ctx context.Context, rawToken, fullMethod string) (*authenticationpb.Session, *serviceAccount, error) {
	unverified, err := readUnverifiedClaims(rawToken)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unauthenticated, "parsing server token: %v", err).Err()
	}
	// The issuer is checked whole when the token is verified.
	account, ok := i.serviceAccountIDToAccount[strings.TrimPrefix(unverified.Issuer, serviceAccountPrefix)]
	if !ok {
		return nil, nil, status.Errorf(codes.Unauthenticated, "server token of unknown service account %q", unverified.Issuer).Err()
	}
	keyOption, err := account.keys.verifyOption(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "service account %q: %v", account.config.GetId(), err).Err()
	}
	verifiedToken, err := jwt.Parse([]byte(rawToken),
		keyOption,
		jwt.WithIssuer(account.subject),
		jwt.WithAudience(serviceName(fullMethod)),
		jwt.WithRequiredClaim(jwt.ExpirationKey),
	)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unauthenticated, "verifying server token: %v", err).Err()
	}
	session, err := carriedSession(verifiedToken)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unauthenticated, "server token of %q: %v", account.config.GetId(), err).Err()
	}
	session.Caller = proto.CloneOf(account.principal())
	return session, account, nil
}

// carriedSession reads the session a server's token carries.
func carriedSession(token jwt.Token) (*authenticationpb.Session, error) {
	claim, _ := token.Get(sessionClaim)
	encoded, ok := claim.(string)
	if !ok {
		return nil, fmt.Errorf("no %s claim", sessionClaim)
	}
	sessionBytes, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding %s claim: %w", sessionClaim, err)
	}
	session := &authenticationpb.Session{}
	if err := pbutil.Unmarshal(sessionBytes, session); err != nil {
		return nil, fmt.Errorf("unmarshaling %s claim: %w", sessionClaim, err)
	}
	return session, nil
}

// serviceName is the service of a full method: "/library.v1.LibraryService/GetBook" -> "library.v1.LibraryService".
func serviceName(fullMethod string) string {
	service, _, _ := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	return service
}
