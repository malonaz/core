package grpc

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// ownCredentials marks the calls of the connections that carry them.
type ownCredentials struct{}

func (ownCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"x-own": "true"}, nil
}
func (ownCredentials) RequireTransportSecurity() bool { return false }

// recordingHealth answers health checks, recording whether each carried the own servers' credentials.
type recordingHealth struct {
	*health.Server
	own []bool
}

func (h *recordingHealth) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	h.own = append(h.own, len(md.Get("x-own")) == 1)
	return h.Server.Check(ctx, request)
}

func serveHealth(t *testing.T, network, address string) (*recordingHealth, net.Addr) {
	t.Helper()
	listener, err := net.Listen(network, address)
	require.NoError(t, err)
	server := grpc.NewServer()
	recording := &recordingHealth{Server: health.NewServer()}
	healthpb.RegisterHealthServer(server, recording)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return recording, listener.Addr()
}

func check(t *testing.T, dialer *Dialer, opts *ClientOpts) {
	t.Helper()
	ctx := context.Background()
	connection, err := dialer.Dial(ctx, opts)
	require.NoError(t, err)
	_, err = healthpb.NewHealthClient(connection.Get()).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}

// Only the connections to the binary's own servers carry what it gives them, however the endpoint was named.
func TestDialer(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	ownSocket, remoteSocket := filepath.Join(directory, "own.socket"), filepath.Join(directory, "remote.socket")
	own, _ := serveHealth(t, "unix", ownSocket)
	remote, _ := serveHealth(t, "unix", remoteSocket)
	dialer := NewDialer(nil, nil, &ServerOpts{SocketPath: ownSocket, DisableTLS: true}).WithServerOptions(grpc.WithPerRPCCredentials(ownCredentials{}))
	t.Cleanup(dialer.Close)

	check(t, dialer, &ClientOpts{SocketPath: ownSocket, DisableTLS: true})
	// Named by a url, without the server's plaintext: the server's own opts win.
	ownByURL, err := ParseClientOpts("unix:" + ownSocket)
	require.NoError(t, err)
	ownByURL.DisableTLS = false
	check(t, dialer, ownByURL)
	check(t, dialer, &ClientOpts{SocketPath: remoteSocket, DisableTLS: true})
	require.Equal(t, []bool{true, true}, own.own)
	require.Equal(t, []bool{false}, remote.own)

	first, err := dialer.Dial(ctx, &ClientOpts{SocketPath: ownSocket, DisableTLS: true})
	require.NoError(t, err)
	second, err := dialer.Dial(ctx, ownByURL)
	require.NoError(t, err)
	require.Same(t, first, second, "one connection per endpoint")
}

// A client given by flags the port of one of the binary's own servers, with no host or localhost, as onikisu's
// ai-engine dials its external server for reflection, reaches it as that server.
func TestDialerOwnPortNamedByFlags(t *testing.T) {
	own, address := serveHealth(t, "tcp", ":0")
	port := address.(*net.TCPAddr).Port
	dialer := NewDialer(nil, nil, &ServerOpts{Port: port, DisableTLS: true}).WithServerOptions(grpc.WithPerRPCCredentials(ownCredentials{}))
	t.Cleanup(dialer.Close)
	check(t, dialer, &ClientOpts{Port: port})
	check(t, dialer, &ClientOpts{Host: "localhost", Port: port})
	require.Equal(t, []bool{true, true}, own.own)
}
