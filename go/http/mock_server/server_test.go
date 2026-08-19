package mockserver_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	mockserver "github.com/malonaz/core/go/http/mock_server"
)

// recorder collects the failures a mock reports, standing in for *testing.T so that the unhappy
// paths can be asserted on rather than failing the test that exercises them.
type recorder struct {
	failures []string
}

func (r *recorder) Errorf(format string, arguments ...any) {
	r.failures = append(r.failures, format)
}

func (r *recorder) Helper() {}

func newTestServer(t *testing.T) *mockserver.Server {
	t.Helper()
	server := mockserver.NewServer()
	t.Cleanup(server.Close)
	return server
}

func get(t *testing.T, server *mockserver.Server, path string) *http.Response {
	t.Helper()
	response, err := http.Get(server.URL() + path)
	require.NoError(t, err)
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func body(t *testing.T, response *http.Response) string {
	t.Helper()
	contents, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return string(contents)
}

func TestGet(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/jwks").Return(`{"keys":[]}`)

	response := get(t, server, "/jwks")
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))
	require.JSONEq(t, `{"keys":[]}`, body(t, response))

	require.True(t, server.AssertExpectations(t))
	requests := server.Requests()
	require.Len(t, requests, 1)
	require.Equal(t, "/jwks", requests[0].Path)
}

// Structs are marshaled to JSON, which is the common case for a third party's REST reply.
func TestReturnStruct(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/user").Return(map[string]any{"id": "abc"})

	response := get(t, server, "/user")
	require.JSONEq(t, `{"id":"abc"}`, body(t, response))
}

// Byte slices are served untouched, for callers that decode something other than JSON.
func TestReturnBytes(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/logo.png").Return([]byte{0x89, 'P', 'N', 'G'}).WithContentType("image/png")

	response := get(t, server, "/logo.png")
	require.Equal(t, "image/png", response.Header.Get("Content-Type"))
	require.Equal(t, "\x89PNG", body(t, response))
}

func TestWithBody(t *testing.T) {
	server := newTestServer(t)
	server.Expect().
		Post("/messages").
		WithBody(`{"text":"hello","recipient":"alice"}`).
		Return(`{"sent":true}`)

	// Field order differs from the expectation: JSON bodies are compared semantically.
	response, err := http.Post(
		server.URL()+"/messages", "application/json",
		strings.NewReader(`{"recipient":"alice","text":"hello"}`),
	)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.JSONEq(t, `{"sent":true}`, body(t, response))
	require.True(t, server.AssertExpectations(t))
}

func TestWithBodyDoesNotMatch(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Post("/messages").WithBody(`{"text":"hello"}`)

	response, err := http.Post(server.URL()+"/messages", "application/json", strings.NewReader(`{"text":"bye"}`))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusNotImplemented, response.StatusCode)
}

func TestWithQuery(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/search").WithQuery("q", "onikisu").Return(`["hit"]`)

	matching := get(t, server, "/search?q=onikisu")
	require.Equal(t, http.StatusOK, matching.StatusCode)

	nonMatching := get(t, server, "/search?q=other")
	require.Equal(t, http.StatusNotImplemented, nonMatching.StatusCode)
}

// Wildcards let one expectation cover a client that versions its own URLs.
func TestWildcardPath(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("*/users/search").Return(`[]`).AnyTimes()
	server.Expect().Get("/rest/*/issues/*").Return(`{}`).AnyTimes()

	require.Equal(t, http.StatusOK, get(t, server, "/rest/api/3/users/search").StatusCode)
	require.Equal(t, http.StatusOK, get(t, server, "/users/search").StatusCode)
	require.Equal(t, http.StatusOK, get(t, server, "/rest/api/2/issues/ABC-1").StatusCode)

	// Anchoring still applies either side of a wildcard.
	require.Equal(t, http.StatusNotImplemented, get(t, server, "/users/search/extra").StatusCode)
	require.Equal(t, http.StatusNotImplemented, get(t, server, "/api/3/issues/ABC-1").StatusCode)
}

func TestWithStatus(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/rate-limited").WithStatus(http.StatusTooManyRequests).Return(`{"error":"slow down"}`)

	response := get(t, server, "/rate-limited")
	require.Equal(t, http.StatusTooManyRequests, response.StatusCode)
}

// Expectations are consumed in order, so the same path can reply differently per call.
func TestTimesOrdersResponses(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/poll").Return(`{"done":false}`).Times(1)
	server.Expect().Get("/poll").Return(`{"done":true}`).Times(1)

	require.JSONEq(t, `{"done":false}`, body(t, get(t, server, "/poll")))
	require.JSONEq(t, `{"done":true}`, body(t, get(t, server, "/poll")))
	require.True(t, server.AssertExpectations(t))
}

func TestMethodIsMatched(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Post("/resource").Return(`{}`)

	response := get(t, server, "/resource")
	require.Equal(t, http.StatusNotImplemented, response.StatusCode)
}

func TestFallback(t *testing.T) {
	server := newTestServer(t)
	require.NoError(t, server.SetFallback(http.StatusOK, "{}"))

	response := get(t, server, "/anything")
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.JSONEq(t, `{}`, body(t, response))

	// A fallback makes the mock a catch-all sink, so unmatched requests are not failures.
	require.True(t, server.AssertExpectations(t))
	require.Len(t, server.Requests(), 1)
}

func TestAssertExpectationsReportsUnmetExpectation(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/never-called")

	recorder := &recorder{}
	require.False(t, server.AssertExpectations(recorder))
	require.Len(t, recorder.failures, 1)
}

func TestAssertExpectationsReportsUnexpectedRequest(t *testing.T) {
	server := newTestServer(t)

	response := get(t, server, "/unexpected")
	require.Equal(t, http.StatusNotImplemented, response.StatusCode)

	recorder := &recorder{}
	require.False(t, server.AssertExpectations(recorder))
	require.Len(t, recorder.failures, 1)
}

func TestAnyTimesIsOptional(t *testing.T) {
	server := newTestServer(t)
	server.Expect().Get("/optional").AnyTimes()
	require.True(t, server.AssertExpectations(t))
}

// Reset clears expectations and history but leaves the fallback, which belongs to the
// environment rather than to a test.
func TestReset(t *testing.T) {
	server := newTestServer(t)
	require.NoError(t, server.SetFallback(http.StatusOK, "{}"))
	server.Expect().Get("/once").Return(`{"a":1}`)
	get(t, server, "/once")

	server.Reset()
	require.Empty(t, server.Requests())
	require.True(t, server.AssertExpectations(t))

	response := get(t, server, "/once")
	require.JSONEq(t, `{}`, body(t, response))
}
