package grpcwebproxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
