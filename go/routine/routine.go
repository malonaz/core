package routine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// PermanentError is a permanent error that will cause a routine to immediately panic.
type PermanentError struct{ Err error }

// Error immplements the error interface.
func (e *PermanentError) Error() string { return fmt.Sprintf("permanent error: %v", e.Err) }

// Is is used used by errors.Is() to match correctly.
func (e *PermanentError) Is(err error) bool {
	_, ok := err.(*PermanentError)
	return ok
}

// NewPermanentError instantiates and returns a new permanent error.
func NewPermanentError(message string, args ...any) *PermanentError {
	return &PermanentError{Err: fmt.Errorf(message, args...)}
}

// FN is a routine function.
type FN func(context.Context) error

// Routine is a wrapper around some function that needs to be executed in a loop in a go routine.
type Routine struct {
	log *slog.Logger

	// Required fields.
	name             string
	fn               FN
	onPermanentError func(error)
	exited           chan struct{}
	closeErr         error
	closeOnce        sync.Once
	cancel           context.CancelFunc
	retryChannel     chan struct{}

	// Additional fields.
	metrics              *routineMetrics
	timeout              time.Duration
	constantBackOff      *backoff.ConstantBackOff
	ticker               *time.Ticker
	signals              []reflect.SelectCase
	maxConsecutiveErrors int
}

// New instantiates and return a new Routine.
func New(name string, fn FN) *Routine {
	return &Routine{
		log:          slog.Default(),
		name:         name,
		fn:           fn,
		exited:       make(chan struct{}),
		retryChannel: make(chan struct{}, 1), // non-blocking writes.
	}
}

func (r *Routine) WithLogger(logger *slog.Logger) *Routine {
	r.log = logger
	return r
}

// Checks the health of this routine.
func (r *Routine) HealthCheck() error {
	select {
	case <-r.exited:
		return fmt.Errorf("routine %q stopped: %w", r.name, r.closeErr)
	default:
		return nil
	}
}

// WithMetrics
func (r *Routine) WithMetrics() *Routine {
	r.metrics = getMetrics()
	return r
}

func (r *Routine) OnPermanentError(fn func(error)) {
	r.onPermanentError = fn
}

// WithMaxConsecutiveErrors sets a max consecutive error threshold which, if exceeded, kills the routine.
func (r *Routine) WithMaxConsecutiveErrors(n int) *Routine {
	r.maxConsecutiveErrors = n
	return r
}

// WithTimeout sets a timeout on the context for each execution of the routine's FN.
func (r *Routine) WithTimeout(d time.Duration) *Routine { r.timeout = d; return r }

func (r *Routine) WithTicker(duration time.Duration) *Routine {
	if r.ticker != nil {
		panic("WithTicker called twice")
	}
	r.ticker = time.NewTicker(duration)
	signal := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.ticker.C)}
	r.signals = append(r.signals, signal)
	return r
}

// WithSignal allows a signal to trigger a run of the routine function.
func (r *Routine) WithSignal(channels ...<-chan struct{}) *Routine {
	for _, channel := range channels {
		signal := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(channel)}
		r.signals = append(r.signals, signal)
	}
	return r
}

// WithConstantBackOff adds a constant backoff everytime a non permanent error is encountered.
func (r *Routine) WithConstantBackOff(seconds int) *Routine {
	r.constantBackOff = backoff.NewConstantBackOff(time.Duration(seconds) * time.Second)
	return r
}

func (r *Routine) Start(ctx context.Context) *Routine {
	go func() {
		var err error
		func() {
			defer func() {
				if v := recover(); v != nil {
					err = NewPermanentError("panic: %v", v)
				}
			}()
			err = r.start(ctx)
		}()
		r.closeErr = err
		close(r.exited)
		r.Close()
		if r.onPermanentError != nil && errors.Is(err, &PermanentError{}) {
			r.onPermanentError(err)
		}
	}()
	return r
}

func (r *Routine) start(ctx context.Context) error {
	if r.metrics != nil {
		r.metrics.running.WithLabelValues(r.name).Set(1)
		defer r.metrics.running.WithLabelValues(r.name).Set(0)
	}

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.log = r.log.With("routine", r.name)
	r.log.InfoContext(ctx, "started routine")
	consecutiveErrors := 0
	fn := func(ctx context.Context) error {
		if err := r.execute(ctx); err != nil {
			consecutiveErrors++
			if r.maxConsecutiveErrors != 0 && consecutiveErrors >= r.maxConsecutiveErrors {
				return NewPermanentError("exceeded max consecutive errors (%d): %w", r.maxConsecutiveErrors, err)
			}
			return err
		}
		consecutiveErrors = 0
		return nil
	}

	// Function to fan out signals into a signal.
	signal := make(chan struct{}, 1)
	go func() {
		if len(r.signals) == 0 {
			// No signals configured — close so receives never block.
			close(signal)
			return
		}

		// Prepend ctx.Done() as case 0 so we can exit when the context is cancelled.
		// Without this, reflect.Select blocks on r.signals indefinitely, leaking this goroutine.
		doneCase := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
		cases := append([]reflect.SelectCase{doneCase}, r.signals...)

		for {
			// reflect.Select blocks until one of the cases fires.
			// `chosen` is the index into `cases` that was selected.
			chosen, _, _ := reflect.Select(cases)
			if chosen == 0 {
				// ctx.Done() fired — stop fanning out signals.
				return
			}

			// One of the configured signals fired — forward it.
			// Non-blocking write: if there's already a pending signal, skip.
			select {
			case signal <- struct{}{}:
			default:
			}
		}
	}()

	for {
		if err := fn(ctx); err != nil {
			select {
			case <-ctx.Done():
				r.log.InfoContext(ctx, "context done", "error", ctx.Err())
				return ctx.Err()
			default:
			}

			if errors.Is(err, &PermanentError{}) {
				r.log.ErrorContext(ctx, "exiting due to permanent error", "error", err)
				return err
			}
			r.log.ErrorContext(ctx, "executing fn", "error", err)
			if r.constantBackOff != nil {
				time.Sleep(r.constantBackOff.NextBackOff())
			}
			// Add a retry signal.
			select {
			case r.retryChannel <- struct{}{}:
			default:
			}
		}

		select {
		case <-ctx.Done():
			r.log.InfoContext(ctx, "context done", "error", ctx.Err())
			return ctx.Err()
		case <-signal:
			r.log.DebugContext(ctx, "received signal")
		case <-r.retryChannel:
			r.log.DebugContext(ctx, "retrying")
		}
	}
}

// Close closes this routine. It is a blocking call guaranteeing that the routine has exited its loop by the time it returns.
func (r *Routine) Close() {
	r.closeOnce.Do(func() {
		r.log.Info("closing")
		if r.cancel != nil {
			r.cancel()
		}
		<-r.exited
		r.log.Info("closed")
		if r.ticker != nil {
			r.ticker.Stop()
		}
	})
}

func (r *Routine) execute(ctx context.Context) error {
	var cancel func()
	if r.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	var start time.Time
	if r.metrics != nil {
		start = time.Now()
	}

	err := r.fn(ctx)

	if r.metrics != nil {
		r.metrics.durationSeconds.WithLabelValues(r.name).Observe(time.Since(start).Seconds())
		status := "success"
		if err != nil {
			status = "error"
			if errors.Is(err, &PermanentError{}) {
				status = "permanent_error"
			}
		}
		r.metrics.executionsTotal.WithLabelValues(r.name, status).Inc()
	}

	return err
}
