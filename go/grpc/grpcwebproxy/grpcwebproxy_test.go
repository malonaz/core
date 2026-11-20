package grpcwebproxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// response is what the client sees, and the path the grpc-web handler routed.
type response struct {
	Path            string
	ContentEncoding string
	Body            string
}

func serve(t *testing.T, opts *Opts, path, acceptEncoding string, body string) response {
	t.Helper()
	var routed string
	handler := wrapHTTP(opts, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routed = r.URL.Path
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, body)
		w.(http.Flusher).Flush()
	}))
	request := httptest.NewRequest(http.MethodPost, path, nil)
	if acceptEncoding != "" {
		request.Header.Set("Accept-Encoding", acceptEncoding)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	data := recorder.Body.Bytes()
	if recorder.Header().Get("Content-Encoding") == "gzip" {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		require.NoError(t, err)
		data, err = io.ReadAll(reader)
		require.NoError(t, err)
	}
	return response{Path: routed, ContentEncoding: recorder.Header().Get("Content-Encoding"), Body: string(data)}
}

func TestWrapHTTPStripsPathPrefixes(t *testing.T) {
	opts := &Opts{StripPathPrefixes: []string{"/v3/gateway", "/gateway/"}, DisableCompression: true}
	for path, expected := range map[string]string{
		"/gateway/pkg.Service/Method":    "/pkg.Service/Method",
		"/v3/gateway/pkg.Service/Method": "/pkg.Service/Method",
		// Not a whole segment.
		"/gateway.pkg.Service/Method": "/gateway.pkg.Service/Method",
		"/pkg.Service/Method":         "/pkg.Service/Method",
	} {
		require.Equal(t, response{Path: expected, Body: "ok"}, serve(t, opts, path, "", "ok"), path)
	}
}

func TestWrapHTTPCompresses(t *testing.T) {
	body := strings.Repeat("grpc-web message ", 100)
	require.Equal(t, response{Path: "/pkg.Service/Method", ContentEncoding: "gzip", Body: body}, serve(t, &Opts{}, "/pkg.Service/Method", "gzip, br", body))
	// Only for a client that accepts it.
	require.Equal(t, response{Path: "/pkg.Service/Method", Body: body}, serve(t, &Opts{}, "/pkg.Service/Method", "", body))
	require.Equal(t, response{Path: "/pkg.Service/Method", Body: body}, serve(t, &Opts{DisableCompression: true}, "/pkg.Service/Method", "gzip", body))
}

// A server stream's messages reach the client as they are flushed, not when the response ends.
func TestWrapHTTPCompressesStreams(t *testing.T) {
	read := make(chan struct{})
	server := httptest.NewServer(wrapHTTP(&Opts{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc-web+proto")
		io.WriteString(w, "first|")
		w.(http.Flusher).Flush()
		<-read
		io.WriteString(w, "second")
	})))
	t.Cleanup(server.Close)

	request, err := http.NewRequest(http.MethodPost, server.URL+"/pkg.Service/Method", nil)
	require.NoError(t, err)
	// Set by hand, the transport leaves the body compressed.
	request.Header.Set("Accept-Encoding", "gzip")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, "gzip", response.Header.Get("Content-Encoding"))
	reader, err := gzip.NewReader(response.Body)
	require.NoError(t, err)

	first := make([]byte, len("first|"))
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(reader, first)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the first message was held back until the response ended")
	}
	close(read)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "first|second", string(first)+string(rest))
}
