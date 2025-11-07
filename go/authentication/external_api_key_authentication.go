package authentication

import (
	"context"
	"fmt"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	authenticationpb "github.com/malonaz/core/genproto/authentication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metadataKeyAPIKey = "x-api-key"

// WithAPIKey creates a new context with the API key set in outgoing metadata.
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	md := metadata.Pairs(metadataKeyAPIKey, apiKey)
	return metadata.NewOutgoingContext(ctx, md)
}

type ExternalApiKeysOpts struct {
	APIKeys []string `long:"api-keys" env:"API_KEYS" env-delim:"," description:"List of service_account_id:api_key pairs" required:"true"`
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

	for _, keyPair := range o.APIKeys {
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
	Config string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

type ExternalApiKeyAuthenticationInterceptor struct {
	apiKeyToServiceAccountID map[string]string
	serviceAccountIDSet      map[string]struct{}
}

func NewExternalApiKeyAuthenticationInterceptor(opts *ExternalApiKeyAuthenticationInterceptorOpts) (*ExternalApiKeyAuthenticationInterceptor, error) {
	// Parse API keys
	apiKeyToServiceAccountID, err := opts.parse()
	if err != nil {
		return nil, err
	}

	configuration, err := parseAuthenticationConfig(opts.Config)
	if err != nil {
		return nil, err
	}

	// Build service account id set.
	serviceAccountIDSet := map[string]struct{}{}
	for _, serviceAccount := range configuration.ServiceAccounts {
		serviceAccountIDSet[serviceAccount.Id] = struct{}{}
	}

	// Validate that all configured service account IDs exist in the configuration
	for _, serviceAccountID := range apiKeyToServiceAccountID {
		if _, ok := serviceAccountIDSet[serviceAccountID]; !ok {
			return nil, fmt.Errorf("service account ID %s not found in configuration", serviceAccountID)
		}
	}

	return &ExternalApiKeyAuthenticationInterceptor{
		apiKeyToServiceAccountID: apiKeyToServiceAccountID,
		serviceAccountIDSet:      serviceAccountIDSet,
	}, nil
}

func (i *ExternalApiKeyAuthenticationInterceptor) authenticateAPIKey(ctx context.Context) (context.Context, error) {
	values := metadata.ValueFromIncomingContext(ctx, metadataKeyAPIKey)
	if len(values) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing api key")
	}

	apiKey := values[0]
	if apiKey == "" {
		return nil, status.Errorf(codes.Unauthenticated, "empty api key")
	}

	serviceAccountID, ok := i.apiKeyToServiceAccountID[apiKey]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "invalid api key")
	}

	if _, ok := i.serviceAccountIDSet[serviceAccountID]; !ok {
		return nil, status.Errorf(codes.Internal, "service account %s not found", serviceAccountID)
	}

	// Create a session with the service account and its roles
	session := &authenticationpb.Session{
		ServiceAccountId: serviceAccountID,
	}

	// Inject the session into the local context
	return InjectSession(ctx, session)
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
