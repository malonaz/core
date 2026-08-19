package mockserver

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"github.com/malonaz/core/go/mock"
)

// Recorder declares expectations on a Server.
type Recorder struct {
	server *Server
}

// Get registers an expectation for a GET of the exact path.
func (r *Recorder) Get(path string) *Call { return r.Method(http.MethodGet, path) }

// Post registers an expectation for a POST of the exact path.
func (r *Recorder) Post(path string) *Call { return r.Method(http.MethodPost, path) }

// Put registers an expectation for a PUT of the exact path.
func (r *Recorder) Put(path string) *Call { return r.Method(http.MethodPut, path) }

// Patch registers an expectation for a PATCH of the exact path.
func (r *Recorder) Patch(path string) *Call { return r.Method(http.MethodPatch, path) }

// Delete registers an expectation for a DELETE of the exact path.
func (r *Recorder) Delete(path string) *Call { return r.Method(http.MethodDelete, path) }

// Method registers an expectation for any verb. Paths match exactly, on the path alone: use
// WithQuery to constrain the query string. A path may contain '*' wildcards, each matching any
// run of characters including slashes, for clients that version their own URLs:
// "*/users/search" matches "/rest/api/3/users/search".
//
// Expectations are matched in declaration order, and one declared with Times stops matching
// once it is exhausted, so several expectations on the same path reply in the order they were
// declared.
func (r *Recorder) Method(method, path string) *Call {
	call := &Call{
		Cardinality: mock.NewCardinality(),
		method:      method,
		path:        path,
		query:       url.Values{},
		header:      http.Header{},
		response:    &response{statusCode: http.StatusOK},
	}
	r.server.mutex.Lock()
	defer r.server.mutex.Unlock()
	r.server.calls = append(r.server.calls, call)
	return call
}

// Call is an expectation on a method and path.
type Call struct {
	mock.Cardinality
	method string
	path   string
	query  url.Values
	header http.Header
	body   []byte
	// bodySet distinguishes an expectation of an empty body from no expectation at all.
	bodySet  bool
	response *response
	err      error
}

// WithBody restricts the expectation to requests carrying the given body. Protos, structs and
// strings holding JSON are compared semantically rather than byte for byte; anything else is
// compared exactly. Without it, any body matches.
func (c *Call) WithBody(body any) *Call {
	encoded, _, err := encodeBody(body)
	if err != nil {
		c.err = fmt.Errorf("encoding expected body: %w", err)
		return c
	}
	c.body = encoded
	c.bodySet = true
	return c
}

// WithQuery restricts the expectation to requests carrying the given query parameter.
func (c *Call) WithQuery(key, value string) *Call {
	c.query.Add(key, value)
	return c
}

// WithHeader restricts the expectation to requests carrying the given header.
func (c *Call) WithHeader(key, value string) *Call {
	c.header.Add(key, value)
	return c
}

// Return sets the body to reply with, inferring the content type: protos and structs are
// marshaled to JSON, byte slices are served as-is. Without it, an empty 200 is returned.
func (c *Call) Return(body any) *Call {
	encoded, contentType, err := encodeBody(body)
	if err != nil {
		c.err = fmt.Errorf("encoding response body: %w", err)
		return c
	}
	c.response.body = encoded
	if c.response.contentType == "" {
		c.response.contentType = contentType
	}
	return c
}

// WithStatus sets the status code to reply with, which defaults to 200.
func (c *Call) WithStatus(statusCode int) *Call {
	c.response.statusCode = statusCode
	return c
}

// WithContentType overrides the content type inferred by Return.
func (c *Call) WithContentType(contentType string) *Call {
	c.response.contentType = contentType
	return c
}

// Times requires the expectation to be matched exactly count times.
func (c *Call) Times(count int) *Call {
	c.Cardinality.Times(count)
	return c
}

// AnyTimes lets the expectation match any number of times, including none. Use it for endpoints
// a dependency calls on its own schedule, which no test asserts on.
func (c *Call) AnyTimes() *Call {
	c.Cardinality.AnyTimes()
	return c
}

func (c *Call) matches(request *Request) bool {
	if c.method != request.Method || !matchPath(c.path, request.Path) {
		return false
	}
	for key, values := range c.query {
		for _, value := range values {
			if !slices.Contains(request.Query[key], value) {
				return false
			}
		}
	}
	for key, values := range c.header {
		for _, value := range values {
			if !slices.Contains(request.Header.Values(key), value) {
				return false
			}
		}
	}
	if c.bodySet && !bodiesEqual(c.body, request.Body) {
		return false
	}
	return true
}
