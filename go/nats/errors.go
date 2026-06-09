package nats

import "time"

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
