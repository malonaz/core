package authentication

import (
  "context"
  "encoding/json"
  "fmt"
  "strings"
  "time"

  grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
  grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
  "github.com/lestrrat-go/jwx/v2/jwk"
  "github.com/lestrrat-go/jwx/v2/jwt"
  "google.golang.org/grpc"
  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/metadata"
  "google.golang.org/protobuf/types/known/structpb"
  "google.golang.org/protobuf/types/known/timestamppb"

  authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
  "github.com/malonaz/core/go/grpc/middleware"
  "github.com/malonaz/core/go/grpc/status"
)

type JwtAuthenticationInterceptorOpts struct {
  MetadataHeader string `long:"metadata-header" env:"METADATA_HEADER" description:"The header for JWT authentication" default:"authorization"`
  Config         string `long:"config" env:"CONFIG" description:"Path to the JWT authentication configuration file" required:"true"`
}

type jwtIssuer struct {
  config   *authenticationpb.JwtIssuer
  keyCache *jwk.Cache
}

type JwtAuthenticationInterceptor struct {
  opts           *JwtAuthenticationInterceptorOpts
  sessionManager *SessionManager
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
    keyCache := jwk.NewCache(ctx)
    if err := keyCache.Register(issuerConfig.JwksUri, jwk.WithMinRefreshInterval(15*time.Minute)); err != nil {
      return nil, fmt.Errorf("registering JWKS URI %q for issuer %q: %w", issuerConfig.JwksUri, issuerConfig.Id, err)
    }
    // Eagerly fetch keys to fail fast on misconfiguration.
    if _, err := keyCache.Refresh(ctx, issuerConfig.JwksUri); err != nil {
      return nil, fmt.Errorf("fetching JWKS from %q for issuer %q: %w", issuerConfig.JwksUri, issuerConfig.Id, err)
    }
    issuerToJwtIssuer[issuerConfig.Issuer] = &jwtIssuer{
      config:   issuerConfig,
      keyCache: keyCache,
    }
  }

  return &JwtAuthenticationInterceptor{
    opts:              opts,
    sessionManager:    sessionManager,
    issuerToJwtIssuer: issuerToJwtIssuer,
  }, nil
}

func (i *JwtAuthenticationInterceptor) authenticateJwt(ctx context.Context) (context.Context, error) {
  values := metadata.ValueFromIncomingContext(ctx, i.opts.MetadataHeader)
  if len(values) == 0 {
    return ctx, nil
  }
  if len(values) != 1 {
    return nil, status.Errorf(codes.Unauthenticated, "expected 1 authorization header, got %d", len(values)).Err()
  }

  bearerToken := values[0]
  if !strings.HasPrefix(bearerToken, "Bearer ") {
    return nil, status.Errorf(codes.Unauthenticated, "authorization header must start with 'Bearer '").Err()
  }
  rawToken := strings.TrimPrefix(bearerToken, "Bearer ")
  if rawToken == "" {
    return nil, status.Errorf(codes.Unauthenticated, "empty bearer token").Err()
  }

  // Parse without verification first to extract the issuer.
  unverifiedToken, err := jwt.Parse([]byte(rawToken), jwt.WithVerify(false))
  if err != nil {
    return nil, status.Errorf(codes.Unauthenticated, "parsing JWT: %v", err).Err()
  }

  jwtIssuer, ok := i.issuerToJwtIssuer[unverifiedToken.Issuer()]
  if !ok {
    return nil, status.Errorf(codes.Unauthenticated, "untrusted issuer %q", unverifiedToken.Issuer()).Err()
  }

  // Fetch the key set from cache.
  keySet, err := jwtIssuer.keyCache.Get(ctx, jwtIssuer.config.JwksUri)
  if err != nil {
    return nil, status.Errorf(codes.Internal, "fetching JWKS: %v", err).Err()
  }

  // Parse and verify the token.
  verifiedToken, err := jwt.Parse([]byte(rawToken),
    jwt.WithKeySet(keySet),
    jwt.WithIssuer(jwtIssuer.config.Issuer),
    jwt.WithAudience(jwtIssuer.config.Audience),
  )
  if err != nil {
    return nil, status.Errorf(codes.Unauthenticated, "verifying JWT: %v", err).Err()
  }

  // Convert claims to structpb.
  claimsMap, err := verifiedToken.AsMap(ctx)
  if err != nil {
    return nil, status.Errorf(codes.Internal, "converting JWT claims to map: %v", err).Err()
  }
  claims, err := structpb.NewStruct(claimsMap)
  if err != nil {
    return nil, status.Errorf(codes.Internal, "converting claims to struct: %v", err).Err()
  }

  // Resolve log fields from claims.
  logFields := resolveLogFields(jwtIssuer.config.LogFieldToClaimsJsonPath, claimsMap)

  sessionMetadata, err := extractSessionMetadataFromContext(ctx)
  if err != nil {
    return nil, err
  }

  session := &authenticationpb.Session{
    CreateTime: timestamppb.Now(),
    Identity: &authenticationpb.Session_JwtIdentity{
      JwtIdentity: &authenticationpb.JwtIdentity{
        Claims:    claims,
        LogFields: logFields,
      },
    },
    Metadata: sessionMetadata,
  }

  signedSession, err := i.sessionManager.sign(session)
  if err != nil {
    return nil, status.Errorf(codes.Internal, "signing session: %v", err).Err()
  }

  ctx = removeFromIncomingContext(ctx, i.opts.MetadataHeader)

  isUpdate := false
  return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// resolveLogFields extracts log field values from claims using JSON paths.
func resolveLogFields(logFieldToClaimsJsonPath map[string]string, claimsMap map[string]any) map[string]string {
  if len(logFieldToClaimsJsonPath) == 0 {
    return nil
  }
  logFields := make(map[string]string, len(logFieldToClaimsJsonPath))
  for logField, jsonPath := range logFieldToClaimsJsonPath {
    value, ok := resolveJsonPath(claimsMap, jsonPath)
    if !ok {
      continue
    }
    logFields[logField] = value
  }
  return logFields
}

// resolveJsonPath traverses a nested map using a dot-separated path.
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

// Unary implements the JWT authentication unary interceptor.
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

// Stream implements the JWT authentication stream interceptor.
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
