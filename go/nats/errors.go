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

// rejectedErrorCodes are the JetStream errors a publish gets for the message
// itself or its stream's configuration, which no retry fixes. Resource and
// availability errors, such as a missing stream mid-cutover, are left out.
var rejectedErrorCodes = map[jetstream.ErrorCode]bool{
	10054: true, // Message size exceeds the stream's maximum.
	10060: true, // Expected stream does not match the subject's.
	10097: true, // Header size exceeds the maximum.
	10109: true, // Stream is sealed.
}

// IsRejected reports whether NATS refused a published message itself, which
// no retry fixes, rather than failing to receive or store it.
func IsRejected(err error) bool {
	if errors.Is(err, nats.ErrMaxPayload) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && rejectedErrorCodes[apiErr.ErrorCode]
}
