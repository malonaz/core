package authentication_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/health"
)

// In core's server chain, as onyx builds it: hooks run after the interceptor, only for calls it let in, and they
// read the session; health checks need no credential.
func TestCoreServerChain(t *testing.T) {
	f := newFixture(t)
	interceptor := f.interceptor(t)
	var hookCalls, hookSessions atomic.Int32
	hook := func(ctx context.Context, _ *librarypb.GetBookRequest) error {
		hookCalls.Add(1)
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
	})
	require.NoError(t, err)
	go server.Serve(context.Background())
	t.Cleanup(func() { server.Stop() })

	p := &platform{interceptor: interceptor}
	connection := p.dial(t, serverOpts.SocketPath, false)
	library := librarypb.NewLibraryServiceClient(connection)

	_, err = library.GetBook(metadata.NewOutgoingContext(context.Background(), apiKey("key-1")), getBookRequest)
	require.NoError(t, err)
	require.EqualValues(t, 1, hookCalls.Load())
	require.EqualValues(t, 1, hookSessions.Load(), "the hook read the session the interceptor set")

	_, err = library.GetBook(context.Background(), getBookRequest)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	require.EqualValues(t, 1, hookCalls.Load(), "no hook runs for a call the interceptor refused")

	healthCheckRequest := &grpc_health_v1.HealthCheckRequest{}
	_, err = grpc_health_v1.NewHealthClient(connection).Check(context.Background(), healthCheckRequest)
	require.NoError(t, err)
}
