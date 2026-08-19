// Package mock holds what the gRPC and HTTP mock servers have in common: how failures are
// reported, and how many times an expectation may and must be matched. The servers themselves
// live beside the packages they stand in for, in go/grpc/mock_server and go/http/mock_server.
package mock

import "fmt"

// TestingT is the subset of *testing.T the mocks report failures through. It is an interface
// rather than the concrete type so that a mock's own tests can assert on the failures it
// reports instead of being failed by them.
type TestingT interface {
	Errorf(format string, arguments ...any)
	Helper()
}

// Cardinality tracks how many times an expectation may and must be matched. The zero value is
// not usable; construct it with NewCardinality, which matches an unlimited number of times and
// requires at least one match.
//
// It is embedded in each server's expectation type, which re-declares Times and AnyTimes to
// return itself so that options stay chainable.
type Cardinality struct {
	// expectedCallCount is the exact number of matches required, or -1 when unspecified.
	expectedCallCount int
	// optional suppresses verification entirely, for expectations that exist only to keep a
	// dependency happy rather than to be asserted on.
	optional  bool
	callCount int
}

// NewCardinality returns a cardinality requiring at least one match, with no upper bound.
func NewCardinality() Cardinality {
	return Cardinality{expectedCallCount: -1}
}

// Times requires the expectation to be matched exactly count times.
func (c *Cardinality) Times(count int) {
	c.expectedCallCount = count
}

// AnyTimes lets the expectation match any number of times, including none.
func (c *Cardinality) AnyTimes() {
	c.optional = true
}

// Exhausted reports whether the expectation has taken every match it was declared for, and so
// should be skipped in favour of later expectations on the same endpoint.
func (c *Cardinality) Exhausted() bool {
	return c.expectedCallCount >= 0 && c.callCount >= c.expectedCallCount
}

// Record counts a match.
func (c *Cardinality) Record() {
	c.callCount++
}

// Verify returns the reason the expectation was not satisfied, or an empty string.
func (c *Cardinality) Verify() string {
	if c.optional {
		return ""
	}
	if c.expectedCallCount >= 0 {
		if c.callCount != c.expectedCallCount {
			return fmt.Sprintf("expected %d call(s), got %d", c.expectedCallCount, c.callCount)
		}
		return ""
	}
	if c.callCount == 0 {
		return "expected at least 1 call, got 0"
	}
	return ""
}
