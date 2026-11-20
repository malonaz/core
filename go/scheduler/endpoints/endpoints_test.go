package endpoints_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"

	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler/endpoints"
)

// On go/authentication/v2, the dispatcher reaches the endpoints its binary serves as its service account, and sends
// nothing to any other: an endpoint served elsewhere sees an anonymous call.
func TestDiscoverAsTheDispatcher(t *testing.T) {
	directory := t.TempDir()
	config := filepath.Join(directory, "authentication.jsonnet")
	require.NoError(t, os.WriteFile(config, []byte(`{
		service_accounts: [{id: "scheduler-dispatcher", grants: [{role: "reflector", scope: "*"}]}],
		roles: [{id: "reflector", rules: [{methods: ["/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"]}]}],
	}`), 0o600))
	interceptor, err := authentication.NewInterceptor(context.Background(), &authentication.Opts{Config: config, ServerSecret: "server-secret-0123456789abcdef01"})
	require.NoError(t, err)
	as, err := interceptor.As("scheduler-dispatcher")
	require.NoError(t, err)
	serve := func(socket string) {
		server := grpcgo.NewServer(grpcgo.ChainStreamInterceptor(interceptor.Stream()))
		reflection.RegisterV1(server)
		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)
		go server.Serve(listener)
		t.Cleanup(server.Stop)
	}
	own, elsewhere := filepath.Join(directory, "own.socket"), filepath.Join(directory, "elsewhere.socket")
	serve(own)
	serve(elsewhere)

	dialer := grpc.NewDialer(nil, nil, &grpc.ServerOpts{SocketPath: own, DisableTLS: true}).
		WithServerOptions(grpcgo.WithPerRPCCredentials(authentication.Credentials()))
	t.Cleanup(dialer.Close)
	ctx := as(context.Background())
	_, err = endpoints.Discover(ctx, dialer, "unix:"+own)
	require.NoError(t, err)
	_, err = endpoints.Discover(ctx, dialer, "unix:"+elsewhere)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
}
