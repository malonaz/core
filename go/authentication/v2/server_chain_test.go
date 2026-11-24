package authentication_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/health"
)

// In core's server chain, as onyx builds it: rewrites run before the interceptor, which decides on the request the
// handler runs, a stream's included; hooks run after it, only for calls it let in, and they read the session; health
// checks need no credential.
func TestCoreServerChain(t *testing.T) {
	f := newFixture(t)
	// Books moved under organizations: onikiri reads organization 1's, and a legacy name only once rewritten.
	f.configuration.ServiceAccounts[0].Grants = []*authenticationpb.Grant{{Role: "reader", Scope: "organizations/1"}}
	public := f.configuration.Roles[0]
	public.Rules = append(public.Rules, &authenticationpb.Rule{Methods: []string{echoStreamMethod}, ConditionCel: `request.message == "canonical"`})
	interceptor := f.interceptor(t)

	var rewrites, rewriteSessions atomic.Int32
	rewriteBook := func(ctx context.Context, request *librarypb.GetBookRequest) error {
		rewrites.Add(1)
		if _, err := authentication.GetSession(ctx); err == nil {
			rewriteSessions.Add(1)
		}
		if strings.HasPrefix(request.GetName(), "shelves/") {
			request.Name = "organizations/1/" + request.GetName()
		}
		return nil
	}
	require.NoError(t, middleware.RegisterHookHandler(rewriteBook, middleware.WithHookRewrite()))
	rewriteEcho := func(_ context.Context, request *mockpb.EchoRequest) error {
		if request.GetMessage() == "legacy" {
			request.Message = "canonical"
		}
		return nil
	}
	require.NoError(t, middleware.RegisterHookHandler(rewriteEcho, middleware.WithHookRewrite()))

	var hookCalls, hookSessions atomic.Int32
	var hookName atomic.Value
	hook := func(ctx context.Context, request *librarypb.GetBookRequest) error {
		hookCalls.Add(1)
		hookName.Store(request.GetName())
		if _, err := authentication.GetSession(ctx); err == nil {
			hookSessions.Add(1)
		}
		return nil
	}
	require.NoError(t, middleware.RegisterHookHandler(hook, middleware.WithHookOnRequest()))

	directory, err := os.MkdirTemp("", "av2")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(directory) })
	serverOpts := &grpc.ServerOpts{
		Health:              &health.GRPCOpts{Interval: time.Second, Timeout: time.Second, Startup: time.Second},
		DisableTLS:          true,
		GracefulStopTimeout: 1,
		SocketPath:          filepath.Join(directory, "library.socket"),
	}
	serverOptions := grpc.ServerOptions{
		UnaryInterceptors:  []grpcgo.UnaryServerInterceptor{interceptor.Unary()},
		StreamInterceptors: []grpcgo.StreamServerInterceptor{interceptor.Stream()},
	}
	server, err := grpc.NewServer(serverOpts, &certs.Opts{}, nil, "library", serverOptions, func(server *grpc.Server) {
		librarypb.RegisterLibraryServiceServer(server.Raw, &library{})
		mockpb.RegisterMockServiceServer(server.Raw, echo{})
	})
	require.NoError(t, err)
	go server.Serve(context.Background())
	t.Cleanup(func() { server.Stop() })

	p := &platform{interceptor: interceptor}
	connection := p.dial(t, serverOpts.SocketPath)
	library := librarypb.NewLibraryServiceClient(connection)
	legacyGetBookRequest := &librarypb.GetBookRequest{Name: "shelves/1/books/1"}

	book, err := library.GetBook(metadata.NewOutgoingContext(context.Background(), apiKey("key-1")), legacyGetBookRequest)
	require.NoError(t, err, "the decision saw the rewritten name")
	require.Equal(t, "organizations/1/shelves/1/books/1", book.GetName())
	require.EqualValues(t, 1, hookCalls.Load())
	require.EqualValues(t, 1, hookSessions.Load(), "the hook read the session the interceptor set")
	require.Equal(t, "organizations/1/shelves/1/books/1", hookName.Load())

	_, err = library.GetBook(context.Background(), legacyGetBookRequest)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	require.EqualValues(t, 1, hookCalls.Load(), "no hook runs for a call the interceptor refused")
	require.EqualValues(t, 2, rewrites.Load(), "rewrites run for every call")
	require.EqualValues(t, 0, rewriteSessions.Load(), "rewrites run before any session")

	mock := mockpb.NewMockServiceClient(connection)
	stream, err := mock.EchoStream(context.Background(), &mockpb.EchoRequest{Message: "legacy"})
	require.NoError(t, err)
	response, err := stream.Recv()
	require.NoError(t, err, "the stream's rule read its first request rewritten")
	require.Equal(t, "canonical", response.GetMessage())
	stream, err = mock.EchoStream(context.Background(), &mockpb.EchoRequest{Message: "other"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)

	healthCheckRequest := &grpc_health_v1.HealthCheckRequest{}
	_, err = grpc_health_v1.NewHealthClient(connection).Check(context.Background(), healthCheckRequest)
	require.NoError(t, err)
}

// echo answers EchoStream with the request it was handed.
type echo struct {
	mockpb.UnimplementedMockServiceServer
}

func (echo) EchoStream(request *mockpb.EchoRequest, stream mockpb.MockService_EchoStreamServer) error {
	return stream.Send(&mockpb.EchoResponse{Message: request.GetMessage()})
}
