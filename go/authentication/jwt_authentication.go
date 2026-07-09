package authentication

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	coregrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
)

type JwtAuthenticationInterceptorOpts struct {
	Config     string `long:"config" env:"CONFIG" description:"Path to the JWT authentication configuration file" required:"true"`
	CookieName string `long:"cookie-name" env:"COOKIE_NAME" description:"Cookie name to read a JWT from when no bearer token is present." default:"access_token"`
}

type jwtIssuer struct {
	config               *authenticationpb.JwtIssuer
	keyCache             *jwk.Cache // non-nil for JWKS URI issuers
	symmetricKey         []byte     // non-nil for symmetric key issuers
	claimsRewriteProgram cel.Program
}

type JwtAuthenticationInterceptor struct {
	opts              *JwtAuthenticationInterceptorOpts
	sessionManager    *SessionManager
	issuerToJwtIssuer map[string]*jwtIssuer
}

func NewJwtAuthenticationInterceptor(
	ctx context.Context,
	opts *JwtAuthenticationInterceptorOpts,
	sessionManager *SessionManager,
) (*JwtAuthenticationInterceptor, error) {
	configuration := &authenticationpb.JwtConfiguration{}
	if err := parseConfig(opts.Config, configuration); err != nil {
		return nil, err
	}

	issuerToJwtIssuer := make(map[string]*jwtIssuer, len(configuration.Issuers))
	for _, issuerConfig := range configuration.Issuers {
		issuer := &jwtIssuer{config: issuerConfig}

		switch keySource := issuerConfig.GetKeySource().(type) {
		case *authenticationpb.JwtIssuer_JwksUri:
			issuer.keyCache = jwk.NewCache(ctx)
			if err := issuer.keyCache.Register(keySource.JwksUri, jwk.WithMinRefreshInterval(15*time.Minute)); err != nil {
				return nil, fmt.Errorf("registering JWKS URI %q for issuer %q: %w", keySource.JwksUri, issuerConfig.Id, err)
			}
			if _, err := issuer.keyCache.Refresh(ctx, keySource.JwksUri); err != nil {
				return nil, fmt.Errorf("fetching JWKS from %q for issuer %q: %w", keySource.JwksUri, issuerConfig.Id, err)
			}
		case *authenticationpb.JwtIssuer_SymmetricKey:
			issuer.symmetricKey = []byte(keySource.SymmetricKey)
		default:
			return nil, fmt.Errorf("issuer %q: key_source is required", issuerConfig.Id)
		}

		if issuerConfig.GetClaimsRewriteCel() != "" {
			environment, err := cel.NewEnv(
				cel.Variable("claims", cel.MapType(cel.StringType, cel.DynType)),
				ext.Protos(),
				ext.Strings(),
			)
			if err != nil {
				return nil, fmt.Errorf("creating CEL environment for issuer %q: %w", issuerConfig.Id, err)
			}
			ast, issues := environment.Compile(issuerConfig.ClaimsRewriteCel)
			if issues != nil && issues.Err() != nil {
				return nil, fmt.Errorf("compiling claims rewrite CEL for issuer %q: %w", issuerConfig.Id, issues.Err())
			}
			issuer.claimsRewriteProgram, err = environment.Program(ast)
			if err != nil {
				return nil, fmt.Errorf("building claims rewrite CEL program for issuer %q: %w", issuerConfig.Id, err)
			}
		}
		issuerToJwtIssuer[issuerConfig.Issuer] = issuer
	}

	return &JwtAuthenticationInterceptor{
		opts:              opts,
		sessionManager:    sessionManager,
		issuerToJwtIssuer: issuerToJwtIssuer,
	}, nil
}

func (i *JwtAuthenticationInterceptor) authenticateJwt(ctx context.Context) (context.Context, error) {
	rawToken, err := i.extractRawToken(ctx)
	if err != nil {
		return nil, err
	}
	if rawToken == "" {
		return ctx, nil
	}

	unverifiedToken, err := jwt.Parse([]byte(rawToken), jwt.WithVerify(false))
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "parsing JWT: %v", err).Err()
	}

	jwtIssuer, ok := i.issuerToJwtIssuer[unverifiedToken.Issuer()]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "untrusted issuer %q", unverifiedToken.Issuer()).Err()
	}

	var verifiedToken jwt.Token
	switch {
	case jwtIssuer.symmetricKey != nil:
		verifiedToken, err = jwt.Parse([]byte(rawToken),
			jwt.WithKey(jwa.HS256, jwtIssuer.symmetricKey),
			jwt.WithIssuer(jwtIssuer.config.Issuer),
			jwt.WithAudience(jwtIssuer.config.Audience),
		)
	case jwtIssuer.keyCache != nil:
		keySet, fetchErr := jwtIssuer.keyCache.Get(ctx, jwtIssuer.config.GetJwksUri())
		if fetchErr != nil {
			return nil, status.Errorf(codes.Internal, "fetching JWKS: %v", fetchErr).Err()
		}
		verifiedToken, err = jwt.Parse([]byte(rawToken),
			jwt.WithKeySet(keySet),
			jwt.WithIssuer(jwtIssuer.config.Issuer),
			jwt.WithAudience(jwtIssuer.config.Audience),
		)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verifying JWT: %v", err).Err()
	}

	claimsJSON, err := json.Marshal(verifiedToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling JWT claims: %v", err).Err()
	}
	claims := &structpb.Struct{}
	if err := claims.UnmarshalJSON(claimsJSON); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling JWT claims into struct: %v", err).Err()
	}

	if jwtIssuer.claimsRewriteProgram != nil {
		output, _, err := jwtIssuer.claimsRewriteProgram.Eval(map[string]any{"claims": claims.AsMap()})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "rewriting claims: %v", err).Err()
		}
		nativeValue, err := output.ConvertToNative(reflect.TypeOf(map[string]any{}))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting rewritten claims to native: %v", err).Err()
		}
		rewrittenJSON, err := json.Marshal(nativeValue)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling rewritten claims: %v", err).Err()
		}
		claims = &structpb.Struct{}
		if err := claims.UnmarshalJSON(rewrittenJSON); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshaling rewritten claims into struct: %v", err).Err()
		}
	}

	sessionMetadata, err := extractSessionMetadataFromContext(ctx)
	if err != nil {
		return nil, err
	}

	session := &authenticationpb.Session{
		CreateTime: timestamppb.Now(),
		Identity: &authenticationpb.Session_JwtIdentity{
			JwtIdentity: &authenticationpb.JwtIdentity{
				Claims:   claims,
				IssuerId: jwtIssuer.config.Id,
			},
		},
		Metadata: sessionMetadata,
	}

	signedSession, err := i.sessionManager.sign(session)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signing session: %v", err).Err()
	}

	ctx = removeFromIncomingContext(ctx, "authorization")

	isUpdate := false
	return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// extractRawToken returns the JWT from the bearer header, falling back to cookies
// (both grpc-gateway binary cookies and grpc-web raw cookie headers).
func (i *JwtAuthenticationInterceptor) extractRawToken(ctx context.Context) (string, error) {
	rawToken, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil && !status.HasCode(err, codes.Unauthenticated) {
		return "", err
	}
	if rawToken != "" {
		return rawToken, nil
	}
	if i.opts.CookieName == "" {
		return "", nil
	}

	// grpc-gateway forwards cookies as binary HttpCookie protos.
	gatewayCookie := &coregrpc.GatewayCookie{}
	httpCookie, err := gatewayCookie.GetHTTPCookie(ctx, i.opts.CookieName)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "reading gateway cookie: %v", err).Err()
	}
	if httpCookie != nil {
		return httpCookie.Value, nil
	}

	// grpc-web clients send the raw Cookie header.
	webCookie := coregrpc.WebCookie{}
	if httpCookie := webCookie.GetHTTPCookie(ctx, i.opts.CookieName); httpCookie != nil {
		return httpCookie.Value, nil
	}
	return "", nil
}

func resolveJsonPath(claimsMap map[string]any, path string) (string, bool) {
	parts := strings.Split(path, ".")
	var current any = claimsMap
	for _, part := range parts {
		currentMap, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		current, ok = currentMap[part]
		if !ok {
			return "", false
		}
	}
	switch value := current.(type) {
	case string:
		return value, true
	case float64:
		return fmt.Sprintf("%v", value), true
	case bool:
		return fmt.Sprintf("%v", value), true
	default:
		bytes, err := json.Marshal(value)
		if err != nil {
			return "", false
		}
		return string(bytes), true
	}
}

func (i *JwtAuthenticationInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := i.authenticateJwt(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
}

func (i *JwtAuthenticationInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := i.authenticateJwt(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
}
