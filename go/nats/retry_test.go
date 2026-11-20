package nats

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// outcome is what a retried setup call returned, and how many times it was attempted.
type outcome struct {
	Result   string
	Err      error
	Attempts int
}

func retry(t *testing.T, ctx context.Context, errs ...error) outcome {
	t.Helper()
	previous := newSetupBackOff
	newSetupBackOff = func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 5)
	}
	t.Cleanup(func() { newSetupBackOff = previous })

	attempts := 0
	result, err := retryUnavailable(ctx, slog.Default(), "creating stream", func() (string, error) {
		attempts++
		if attempts <= len(errs) {
			return "", errs[attempts-1]
		}
		return "stream", nil
	})
	return outcome{Result: result, Err: err, Attempts: attempts}
}

func TestRetryUnavailable(t *testing.T) {
	unavailable := &jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}
	badRequest := &jetstream.APIError{Code: 400, ErrorCode: 10003, Description: "bad request"}

	// Through an election.
	require.Equal(t, outcome{Result: "stream", Attempts: 3}, retry(t, context.Background(), unavailable, unavailable))
	// Anything else fails at once.
	require.Equal(t, outcome{Err: badRequest, Attempts: 1}, retry(t, context.Background(), badRequest))
	// Not forever: the last error once the backoff runs out.
	require.Equal(t, outcome{Err: unavailable, Attempts: 6}, retry(t, context.Background(), unavailable, unavailable, unavailable, unavailable, unavailable, unavailable, unavailable))

	// Nor past the caller's context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, outcome{Err: context.Canceled, Attempts: 1}, retry(t, ctx, unavailable, unavailable))
}
