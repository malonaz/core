package nats

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Action int

const (
	ActionNak Action = iota
	ActionNakWithDelay
	ActionTerm
)

type ProcessingError struct {
	err    error
	action Action
	delay  time.Duration
}

func (e *ProcessingError) Error() string { return e.err.Error() }
func (e *ProcessingError) Unwrap() error { return e.err }

func TermError(err error) *ProcessingError {
	return &ProcessingError{err: err, action: ActionTerm}
}

func NakError(err error) *ProcessingError {
	return &ProcessingError{err: err, action: ActionNak}
}

func NakWithDelayError(err error, delay time.Duration) *ProcessingError {
	return &ProcessingError{err: err, action: ActionNakWithDelay, delay: delay}
}

// IsRejected reports whether NATS refused a published message itself, which
// no retry fixes, rather than failing to receive it.
func IsRejected(err error) bool {
	var apiErr *jetstream.APIError
	return errors.Is(err, nats.ErrMaxPayload) || errors.As(err, &apiErr)
}
