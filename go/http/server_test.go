package http

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// serveHijacked serves a route whose handler hijacks the connection and runs until release closes,
// and returns once a request to it has been hijacked.
func serveHijacked(t *testing.T, gracefulStopTimeout int, release chan struct{}) *Server {
	hijacked := make(chan struct{})
	server := NewServer(&Opts{GracefulStopTimeout: gracefulStopTimeout}, "test", func(server *Server) {
		server.RegisterRoute("/call", func(w http.ResponseWriter, r *http.Request) {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			close(hijacked)
			<-release
		})
	})
	listener, err := server.listen()
	require.NoError(t, err)
	go server.httpServer.Serve(listener)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	_, err = fmt.Fprint(conn, "GET /call HTTP/1.1\r\nHost: test\r\n\r\n")
	require.NoError(t, err)
	<-hijacked
	return server
}

func TestGracefulStopWaitsForHijackedHandlers(t *testing.T) {
	release := make(chan struct{})
	server := serveHijacked(t, 5, release)

	stopped := make(chan error, 1)
	go func() { stopped <- server.GracefulStop() }()
	select {
	case <-stopped:
		t.Fatal("GracefulStop returned before the hijacked handler did")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("GracefulStop did not return once the hijacked handler did")
	}
}

func TestGracefulStopGivesUpOnHijackedHandlersAtTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := serveHijacked(t, 1, release)

	start := time.Now()
	require.NoError(t, server.GracefulStop())
	require.InDelta(t, time.Second, time.Since(start), float64(500*time.Millisecond))
}

// Handlers that are not hijacked release the count too.
func TestGracefulStopAfterRequests(t *testing.T) {
	server := NewServer(&Opts{GracefulStopTimeout: 5}, "test", func(server *Server) {
		server.RegisterRoute("/ok", func(w http.ResponseWriter, r *http.Request) {})
	})
	listener, err := server.listen()
	require.NoError(t, err)
	go server.httpServer.Serve(listener)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = fmt.Fprint(conn, "GET /ok HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)

	start := time.Now()
	require.NoError(t, server.GracefulStop())
	require.Less(t, time.Since(start), time.Second)
}
