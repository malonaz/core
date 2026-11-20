package nats

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// newSetupBackOff paces the retries of a service's JetStream setup. A leader election takes
// seconds; two minutes outlasts a rolling restart of the whole cluster.
var newSetupBackOff = func() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 2 * time.Minute
	return b
}

// retryUnavailable calls fn until it succeeds, fails for another reason than [IsUnavailable], or
// the backoff or ctx runs out, so a service starting mid-election doesn't exit.
func retryUnavailable[T any](ctx context.Context, log *slog.Logger, action string, fn func() (T, error)) (T, error) {
	operation := func() (T, error) {
		result, err := fn()
		if err != nil && !IsUnavailable(err) {
			return result, backoff.Permanent(err)
		}
		return result, err
	}
	notify := func(err error, wait time.Duration) {
		log.Warn(fmt.Sprintf("%s: jetstream unavailable, retrying in %s: %v", action, wait.Round(time.Millisecond), err))
	}
	return backoff.RetryNotifyWithData(operation, backoff.WithContext(newSetupBackOff(), ctx), notify)
}
