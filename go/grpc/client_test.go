package grpc

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/malonaz/core/go/prometheus"
)

// A connection made before its server is bound (an in-process dependency, bound later in start
// order) reaches it on the first call, instead of failing through the reconnect backoff.
func TestConnectBeforeServerIsBound(t *testing.T) {
	ctx := context.Background()
	socketPath := filepath.Join(t.TempDir(), "server.socket")
	connection, err := NewConnection(&ClientOpts{SocketPath: socketPath, DisableTLS: true}, nil, &prometheus.Opts{})
	require.NoError(t, err)
	require.NoError(t, connection.Connect(ctx))
	t.Cleanup(func() { connection.Close() })

	// Nothing is dialed yet: an eager dial would already be failing against the missing socket.
	require.Never(t, func() bool { return connection.Get().GetState() != connectivity.Idle }, 200*time.Millisecond, 10*time.Millisecond)

	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	callCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(connection.Get()).Check(callCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.GetStatus())
}

// Building a balancer option while another goroutine dials must not race on gRPC's resolver registry.
func TestWithDNSBalancerConcurrentWithNewClient(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			WithDNSBalancer()
		}
	}()
	for range 100 {
		conn, err := grpc.NewClient("passthrough:///localhost:0", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn.Close()
	}
	<-done
}

// Every connection carries the metadata a call's context names, computed for each call as it is sent; a context
// naming none sends none.
func TestCallMetadata(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "server.socket")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	received := make(chan []string, 1)
	record := func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		received <- md.Get("x-call")
		return handler(ctx, request)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(record))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := NewConnection(&ClientOpts{SocketPath: socketPath, DisableTLS: true}, nil, &prometheus.Opts{})
	require.NoError(t, err)
	require.NoError(t, connection.Connect(context.Background()))
	t.Cleanup(func() { connection.Close() })
	healthClient := healthpb.NewHealthClient(connection.Get())

	calls := 0
	ctx := WithCallMetadata(context.Background(), func(ctx context.Context) (map[string]string, error) {
		calls++
		requestInfo, _ := credentials.RequestInfoFromContext(ctx)
		return map[string]string{"x-call": fmt.Sprint(requestInfo.Method, " ", calls)}, nil
	})
	for _, expected := range []string{"/grpc.health.v1.Health/Check 1", "/grpc.health.v1.Health/Check 2"} {
		_, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{expected}, <-received)
	}
	_, err = healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Empty(t, <-received)
}
