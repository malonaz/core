package authentication

import (
	"context"
	"fmt"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ExternalApiKeysOpts struct {
	MetadataHeader string `long:"metadata-header" env:"METADATA_HEADER" description:"The header you wish to use for api keys" default:"x-api-key"`
	APIKeys        string `long:"api-keys" env:"API_KEYS" description:"List of service_account_id:api_key pairs" required:"true"`
	Config         string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

func WithAPIKey(ctx context.Context, headerKey, apiKey string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, headerKey, apiKey)
}

// WithAPIKey creates a new context with the API key set in outgoing metadata.
func (o *ExternalApiKeysOpts) WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return WithAPIKey(ctx, o.MetadataHeader, apiKey)
}

func (o *ExternalApiKeysOpts) ParseAPIKey(targetServiceAccountID string) (string, error) {
	apiKeyToServiceAccountID, err := o.parse()
	if err != nil {
		return "", err
	}
	var targetAPIKey string
	for apiKey, serviceAccountID := range apiKeyToServiceAccountID {
		if serviceAccountID == targetServiceAccountID {
			targetAPIKey = apiKey
			break
		}
	}
	if targetAPIKey == "" {
		return "", fmt.Errorf("no api key found")
	}
	return targetAPIKey, nil
}

func (o *ExternalApiKeysOpts) parse() (map[string]string, error) {
	apiKeyToServiceAccountID := make(map[string]string)

	apiKeyPairs := strings.SplitSeq(o.APIKeys, ",")
	for keyPair := range apiKeyPairs {
		parts := strings.SplitN(keyPair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid api key format: %s (expected service_account_id:api_key)", keyPair)
		}

		serviceAccountID := strings.TrimSpace(parts[0])
		apiKey := strings.TrimSpace(parts[1])

		if serviceAccountID == "" || apiKey == "" {
			return nil, fmt.Errorf("invalid api key format: %s (service_account_id and api_key cannot be empty)", keyPair)
		}

		if _, ok := apiKeyToServiceAccountID[apiKey]; ok {
			return nil, fmt.Errorf("duplicate api key found: %s", apiKey)
		}

		apiKeyToServiceAccountID[apiKey] = serviceAccountID
	}
	if len(apiKeyToServiceAccountID) == 0 {
		return nil, fmt.Errorf("no valid api keys configured")
	}
	return apiKeyToServiceAccountID, nil
}

type ExternalApiKeyAuthenticationInterceptorOpts struct {
	*ExternalApiKeysOpts
}

type ExternalApiKeyAuthenticationInterceptor struct {
	opts                             *ExternalApiKeyAuthenticationInterceptorOpts
	sessionManager                   *SessionManager
	apiKeyToServiceAccountID         map[string]string
	serviceAccountIDToServiceAccount map[string]*authenticationpb.ServiceAccount
}

func NewExternalApiKeyAuthenticationInterceptor(
	opts *ExternalApiKeyAuthenticationInterceptorOpts,
	sessionManager *SessionManager,
) (*ExternalApiKeyAuthenticationInterceptor, error) {
	// Parse API keys
	apiKeyToServiceAccountID, err := opts.parse()
	if err != nil {
		return nil, err
	}

	configuration := &authenticationpb.ServiceAccountConfiguration{}
	if err := parseConfig(opts.Config, configuration); err != nil {
		return nil, err
	}

	// Build service account lookup map
	serviceAccountIDToServiceAccount := make(map[string]*authenticationpb.ServiceAccount)
	for _, serviceAccount := range configuration.ServiceAccounts {
		serviceAccountIDToServiceAccount[serviceAccount.Id] = serviceAccount
	}

	return &ExternalApiKeyAuthenticationInterceptor{
		opts:                             opts,
		sessionManager:                   sessionManager,
		apiKeyToServiceAccountID:         apiKeyToServiceAccountID,
		serviceAccountIDToServiceAccount: serviceAccountIDToServiceAccount,
	}, nil
}

func (i *ExternalApiKeyAuthenticationInterceptor) authenticateAPIKey(ctx context.Context) (context.Context, error) {
	values := metadata.ValueFromIncomingContext(ctx, i.opts.MetadataHeader)
	if len(values) == 0 {
		// There is no api key so we simply return.
		return ctx, nil
	}
	if len(values) != 1 {
		return nil, status.Errorf(codes.Unauthenticated, "expected 1 api key got %d", len(values))
	}

	apiKey := values[0]
	if apiKey == "" {
		return nil, status.Errorf(codes.Unauthenticated, "empty api key")
	}

	serviceAccountID, ok := i.apiKeyToServiceAccountID[apiKey]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "invalid api key")
	}

	serviceAccount, ok := i.serviceAccountIDToServiceAccount[serviceAccountID]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "could not find service account")
	}

	// Create a session with the service account and its roles
	session := &authenticationpb.Session{
		CreateTime: timestamppb.Now(),
		Identity: &authenticationpb.Session_ServiceAccountIdentity{
			ServiceAccountIdentity: &authenticationpb.ServiceAccountIdentity{
				ServiceAccount: serviceAccount,
			},
		},
		RoleIds: serviceAccount.RoleIds,
	}

	signedSession, err := i.sessionManager.sign(session)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signing session: %v", err)
	}

	// Remove the header from the incoming metadata header.
	ctx = removeFromIncomingContext(ctx, i.opts.MetadataHeader)

	isUpdate := false
	return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// Unary implements the API key authentication unary interceptor.
func (i *ExternalApiKeyAuthenticationInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := i.authenticateAPIKey(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// Stream implements the API key authentication stream interceptor.
func (i *ExternalApiKeyAuthenticationInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := i.authenticateAPIKey(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

func removeFromIncomingContext(ctx context.Context, keys ...string) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	md = md.Copy()
	for _, key := range keys {
		delete(md, key)
	}
	return metadata.NewIncomingContext(ctx, md)
}
