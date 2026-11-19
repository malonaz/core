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

func TestGracefulStopHijacked(t *testing.T) {
	testCases := []struct {
		name                 string
		gracefulStopHijacked bool
		// Whether GracefulStop returns while the hijacked handler still runs.
		expectedStopsEarly bool
	}{
		{name: "ignores hijacked handlers by default", expectedStopsEarly: true},
		{name: "waits for hijacked handlers", gracefulStopHijacked: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			hijacked := make(chan struct{})
			release := make(chan struct{})
			server := NewServer(&Opts{GracefulStopTimeout: 5, GracefulStopHijacked: testCase.gracefulStopHijacked}, "test", func(server *Server) {
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
			defer conn.Close()
			_, err = fmt.Fprint(conn, "GET /call HTTP/1.1\r\nHost: test\r\n\r\n")
			require.NoError(t, err)
			<-hijacked

			stopped := make(chan error, 1)
			go func() { stopped <- server.GracefulStop() }()
			select {
			case err := <-stopped:
				require.True(t, testCase.expectedStopsEarly, "GracefulStop returned before the hijacked handler did")
				require.NoError(t, err)
				close(release)
				return
			case <-time.After(200 * time.Millisecond):
				require.False(t, testCase.expectedStopsEarly, "GracefulStop did not return")
			}
			close(release)
			select {
			case err := <-stopped:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("GracefulStop did not return once the hijacked handler did")
			}
		})
	}
}

// The handler counter is released by handlers that are not hijacked too.
func TestGracefulStopHijackedAfterRequests(t *testing.T) {
	server := NewServer(&Opts{GracefulStopTimeout: 5, GracefulStopHijacked: true}, "test", func(server *Server) {
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
