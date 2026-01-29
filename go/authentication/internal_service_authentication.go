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

type InternalServiceAuthenticationInterceptorOpts struct {
	MetadataHeader        string `long:"metadata-header" env:"METADATA_HEADER" description:"The header for internal service authentication" default:"x-internal-service-auth"`
	InternalServiceSecret string `long:"internal-service-secret" env:"INTERNAL_SERVICE_SECRET" description:"Secret shared by all internal services" required:"true"`
	Config                string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`

	// Runtime.
	serviceAccountIDToServiceAccount map[string]*authenticationpb.ServiceAccount
}

func (o *InternalServiceAuthenticationInterceptorOpts) init() error {
	if o.serviceAccountIDToServiceAccount != nil {
		// Already initialized.
		return nil
	}

	// Parse the configuration.
	configuration := &authenticationpb.ServiceAccountConfiguration{}
	if err := parseConfig(o.Config, configuration); err != nil {
		return err
	}

	// Build service account lookup map
	o.serviceAccountIDToServiceAccount = make(map[string]*authenticationpb.ServiceAccount)
	for _, serviceAccount := range configuration.ServiceAccounts {
		o.serviceAccountIDToServiceAccount[serviceAccount.Id] = serviceAccount
	}
	return nil
}

// WithInternalServiceAuth creates a new context with the internal service auth header in the format service_account_id/secret
func (o *InternalServiceAuthenticationInterceptorOpts) WithServiceAccount(
	serviceAccountID string,
) (func(context.Context) context.Context, error) {
	// Initialize.
	if err := o.init(); err != nil {
		return nil, fmt.Errorf("initializing internal service authentication interceptor: %w", err)
	}
	// Check that the service account exists.
	if _, ok := o.serviceAccountIDToServiceAccount[serviceAccountID]; !ok {
		return nil, fmt.Errorf("service account %s is not defined in config %s", serviceAccountID, o.Config)
	}

	// Construct the outgoing context.
	authValue := serviceAccountID + "/" + o.InternalServiceSecret

	return func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, o.MetadataHeader, authValue)
	}, nil
}

type InternalServiceAuthenticationInterceptor struct {
	opts           *InternalServiceAuthenticationInterceptorOpts
	sessionManager *SessionManager
}

func NewInternalServiceAuthenticationInterceptor(
	opts *InternalServiceAuthenticationInterceptorOpts,
	sessionManager *SessionManager,
) (*InternalServiceAuthenticationInterceptor, error) {

	if err := opts.init(); err != nil {
		return nil, err
	}

	return &InternalServiceAuthenticationInterceptor{
		opts:           opts,
		sessionManager: sessionManager,
	}, nil
}

func (i *InternalServiceAuthenticationInterceptor) authenticateService(ctx context.Context) (context.Context, error) {
	// Check for the internal service auth header
	values := metadata.ValueFromIncomingContext(ctx, i.opts.MetadataHeader)
	if len(values) == 0 {
		// No internal service auth provided, just continue to other auth methods
		return ctx, nil
	}
	if len(values) != 1 {
		return nil, status.Errorf(codes.Unauthenticated, "expected 1 internal service auth value, got %d", len(values))
	}

	// Parse the auth value in format: service_account_id/secret
	authValue := values[0]
	parts := strings.SplitN(authValue, "/", 2)
	if len(parts) != 2 {
		return nil, status.Errorf(codes.Unauthenticated, "invalid internal service auth format, expected: service_account_id/secret")
	}

	serviceAccountID := parts[0]
	secret := parts[1]

	if serviceAccountID == "" {
		return nil, status.Errorf(codes.Unauthenticated, "empty service account ID")
	}

	if secret != i.opts.InternalServiceSecret {
		return nil, status.Errorf(codes.Unauthenticated, "invalid internal service secret")
	}

	serviceAccount, ok := i.opts.serviceAccountIDToServiceAccount[serviceAccountID]
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "could not find service account")
	}

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

	isUpdate := false
	return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// Unary implements the internal service authentication unary interceptor.
func (i *InternalServiceAuthenticationInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := i.authenticateService(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// Stream implements the internal service authentication stream interceptor.
func (i *InternalServiceAuthenticationInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := i.authenticateService(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}
