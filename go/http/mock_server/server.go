// Package mockserver provides an HTTP server that stands in for a third party in tests. It is
// configured through an EXPECT() recorder and verified with AssertExpectations.
package mockserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/mock"
	"github.com/malonaz/core/go/pbutil"
)

// Server serves canned responses, matched on method and path.
//
// Requests that match no expectation get the fallback response when one is registered, and
// otherwise fail with 501 and are reported by AssertExpectations. A fallback turns the mock into
// a catch-all sink, which is what a dependency that makes unpredictable calls of its own during
// startup needs.
type Server struct {
	server *httptest.Server

	mutex              sync.Mutex
	calls              []*Call
	requests           []*Request
	unexpectedRequests []string
	fallback           *response
}

// Request is a single request the server received.
type Request struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   []byte
}

type response struct {
	statusCode  int
	contentType string
	body        []byte
}

// NewServer starts a mock HTTP server on an ephemeral port.
func NewServer() *Server {
	server := &Server{}
	server.server = httptest.NewServer(http.HandlerFunc(server.handle))
	return server
}

// URL returns the base URL clients should be pointed at.
func (s *Server) URL() string { return s.server.URL }

// Close shuts the server down.
func (s *Server) Close() { s.server.Close() }

// EXPECT returns a recorder for declaring expectations.
func (s *Server) EXPECT() *Recorder { return &Recorder{server: s} }

// SetFallback registers the response for requests matching no expectation. Those requests are
// still recorded by Requests, but no longer reported by AssertExpectations. Passing a nil body
// serves an empty 200.
func (s *Server) SetFallback(statusCode int, body any) error {
	fallback, err := newResponse(statusCode, body)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.fallback = fallback
	return nil
}

// Requests returns a copy of every request received so far, including unmatched ones.
func (s *Server) Requests() []*Request {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]*Request(nil), s.requests...)
}

// Reset drops every expectation and recorded request, so each test starts from a clean slate.
// The fallback is left in place, since it belongs to the environment rather than to a test.
func (s *Server) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.calls = nil
	s.requests = nil
	s.unexpectedRequests = nil
}

// AssertExpectations fails the test if any expectation went unmet, or any request matched none
// while no fallback was registered.
func (s *Server) AssertExpectations(t mock.TestingT) bool {
	t.Helper()
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ok := true
	for _, call := range s.calls {
		reason := call.Verify()
		if reason == "" {
			continue
		}
		ok = false
		t.Errorf("mock HTTP: %s %s: %s", call.method, call.path, reason)
	}
	for _, request := range s.unexpectedRequests {
		ok = false
		t.Errorf("mock HTTP: unexpected request %s", request)
	}
	return ok
}

func (s *Server) handle(responseWriter http.ResponseWriter, httpRequest *http.Request) {
	body, _ := io.ReadAll(httpRequest.Body)
	request := &Request{
		Method: httpRequest.Method,
		Path:   httpRequest.URL.Path,
		Query:  httpRequest.URL.Query(),
		Header: httpRequest.Header.Clone(),
		Body:   body,
	}

	matched, err := s.match(request)
	if err != nil {
		http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
		return
	}

	if matched.contentType != "" {
		responseWriter.Header().Set("Content-Type", matched.contentType)
	}
	responseWriter.WriteHeader(matched.statusCode)
	responseWriter.Write(matched.body)
}

// match finds the first unexhausted expectation for the request, recording the request either
// way so that Requests reflects everything the mock saw.
func (s *Server) match(request *Request) (*response, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.requests = append(s.requests, request)

	for _, call := range s.calls {
		if call.Exhausted() || !call.matches(request) {
			continue
		}
		if call.err != nil {
			return nil, call.err
		}
		call.Record()
		return call.response, nil
	}

	if s.fallback != nil {
		return s.fallback, nil
	}
	s.unexpectedRequests = append(s.unexpectedRequests, describeRequest(request))
	return &response{
		statusCode:  http.StatusNotImplemented,
		contentType: "text/plain; charset=utf-8",
		body:        []byte(fmt.Sprintf("no expectation matched %s %s\n", request.Method, request.Path)),
	}, nil
}

func newResponse(statusCode int, body any) (*response, error) {
	encoded, contentType, err := encodeBody(body)
	if err != nil {
		return nil, err
	}
	return &response{statusCode: statusCode, contentType: contentType, body: encoded}, nil
}

// encodeBody renders a body supplied by a caller, along with the content type it implies.
func encodeBody(body any) ([]byte, string, error) {
	switch typedBody := body.(type) {
	case nil:
		return nil, "", nil
	case []byte:
		return typedBody, "application/octet-stream", nil
	case string:
		if json.Valid([]byte(typedBody)) {
			return []byte(typedBody), "application/json", nil
		}
		return []byte(typedBody), "text/plain; charset=utf-8", nil
	case proto.Message:
		encoded, err := pbutil.JSONCamelCaseMarshal(typedBody)
		if err != nil {
			return nil, "", fmt.Errorf("marshaling proto: %w", err)
		}
		return encoded, "application/json", nil
	default:
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, "", fmt.Errorf("marshaling %T: %w", body, err)
		}
		return encoded, "application/json", nil
	}
}

// matchPath matches a request path against a pattern whose '*' wildcards each stand for any run
// of characters, slashes included. Segments either side of a wildcard are anchored, so a pattern
// without one matches exactly.
func matchPath(pattern, path string) bool {
	if !strings.Contains(pattern, "*") {
		return pattern == path
	}
	segments := strings.Split(pattern, "*")
	if !strings.HasPrefix(path, segments[0]) {
		return false
	}
	remaining := path[len(segments[0]):]
	for _, segment := range segments[1 : len(segments)-1] {
		index := strings.Index(remaining, segment)
		if index < 0 {
			return false
		}
		remaining = remaining[index+len(segment):]
	}
	return strings.HasSuffix(remaining, segments[len(segments)-1])
}

// bodiesEqual compares JSON bodies semantically, so that field order and whitespace do not
// decide whether an expectation matches, and everything else byte for byte.
func bodiesEqual(expected, actual []byte) bool {
	if bytes.Equal(expected, actual) {
		return true
	}
	if !json.Valid(expected) || !json.Valid(actual) {
		return false
	}
	var expectedValue, actualValue any
	if err := json.Unmarshal(expected, &expectedValue); err != nil {
		return false
	}
	if err := json.Unmarshal(actual, &actualValue); err != nil {
		return false
	}
	return reflect.DeepEqual(expectedValue, actualValue)
}

// describeRequest renders a request for a failure message, dropping the body when it is not
// printable so that assertion output stays readable.
func describeRequest(request *Request) string {
	description := fmt.Sprintf("%s %s", request.Method, request.Path)
	if len(request.Query) > 0 {
		description += "?" + request.Query.Encode()
	}
	if len(request.Body) > 0 && json.Valid(request.Body) {
		description += " body=" + string(request.Body)
	}
	return description
}
