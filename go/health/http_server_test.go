package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type response struct {
	Code int
	Body string
}

func TestLiveness(t *testing.T) {
	failing := func(context.Context) error { return errors.New("routine stopped") }
	passing := func(context.Context) error { return nil }
	for _, tc := range []struct {
		name     string
		ready    bool
		checks   map[string]Check
		expected response
	}{
		{name: "not ready", expected: response{Code: http.StatusServiceUnavailable, Body: "Server not ready\n"}},
		{name: "ready", ready: true, checks: map[string]Check{"a-processor": passing}, expected: response{Code: http.StatusOK, Body: "ok"}},
		{
			name:     "failing check",
			ready:    true,
			checks:   map[string]Check{"a-processor": failing},
			expected: response{Code: http.StatusServiceUnavailable, Body: "a-processor: routine stopped\n"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(&Opts{GRPCOpts: &GRPCOpts{}}, "test")
			for name, check := range tc.checks {
				s.RegisterLivenessCheck(name, check)
			}
			if tc.ready {
				s.MarkReady()
			}
			recorder := httptest.NewRecorder()
			s.liveness(recorder, httptest.NewRequest(http.MethodGet, "/liveness", nil))
			require.Equal(t, tc.expected, response{Code: recorder.Code, Body: recorder.Body.String()})
		})
	}
}
