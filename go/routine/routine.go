package routine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/malonaz/core/go/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var log = logging.NewLogger()

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
func NewPermanentError(message string, args ...interface{}) *PermanentError {
	return &PermanentError{Err: fmt.Errorf(message, args)}
}

// FN is a routine function.
type FN func(context.Context) error

// Routine is a wrapper around some function that needs to be executed in a loop in a go routine.
type Routine struct {
	// Required fields.
	name         string
	fn           FN
	close        chan struct{}
	closeOnce    sync.Once
	retryChannel chan struct{}

	// Additional fields.
	timeoutSeconds       int
	constantBackOff      *backoff.ConstantBackOff
	ticker               *time.Ticker
	signals              []reflect.SelectCase
	maxConsecutiveErrors int
	errorCounter         prometheus.Counter
	durationHistogram    *prometheus.HistogramVec
}

// New instantiates and return a new Routine.
func New(name string, fn FN) *Routine {
	return &Routine{
		name:         name,
		fn:           fn,
		close:        make(chan struct{}),    // unbuffered to make sure it's blocking write.
		retryChannel: make(chan struct{}, 1), // non-blocking writes.
	}
}

// WithMaxConsecutiveErrors sets a max consecutive error threshold which, if exceeded, triggers a panic.
func (r *Routine) WithMaxConsecutiveErrors(maxConsecutiveErrors int) *Routine {
	r.maxConsecutiveErrors = maxConsecutiveErrors
	return r
}

// WithTimeout sets a timeout on the context for each execution of the routine's FN.
func (r *Routine) WithTimeout(seconds int) *Routine { r.timeoutSeconds = seconds; return r }

// WithTicker sets a ticker internal at which the fn will be executed.
func (r *Routine) WithTickerS(seconds int) *Routine {
	return r.WithTicker(time.Duration(seconds) * time.Second)
}

// WithTickerMs sets a ticker internal in ms at which the fn will be executed.
func (r *Routine) WithTickerMs(ms int64) *Routine {
	return r.WithTicker(time.Duration(ms) * time.Microsecond)
}

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

// WithDurationHistogram sets a routine to measure duration metrics.
func (r *Routine) WithDurationHistogram(name string) *Routine {
	r.durationHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: name,
			Help: "Duration of routine iteration",
		},
		[]string{"success"},
	)
	return r
}

// WithErrorCounter sets a routine to measure number of errors.
func (r *Routine) WithErrorCounter(name string) *Routine {
	r.errorCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: name,
			Help: "Errors returned from routine",
		},
	)
	return r
}

// WithConstantBackOff adds a constant backoff everytime a non permanent error is encountered.
func (r *Routine) WithConstantBackOff(seconds int) *Routine {
	r.constantBackOff = backoff.NewConstantBackOff(time.Duration(seconds) * time.Second)
	return r
}

// Start the routine. Non-block calling call.
func (r *Routine) Start(ctx context.Context) *Routine {
	ctx, cancel := context.WithCancel(ctx)
	log.Infof("started routine: %s", r.name)
	consecutiveErrors := 0
	fn := func(ctx context.Context) error {
		if r.maxConsecutiveErrors != 0 && consecutiveErrors >= r.maxConsecutiveErrors {
			log.Panicf("exceeded max (%d) consecutive errors", r.maxConsecutiveErrors)
		}
		if err := r.execute(ctx); err != nil {
			consecutiveErrors++
			return err
		}
		consecutiveErrors = 0
		return nil
	}

	// Function to fan out signals into a signal.
	signal := make(chan struct{}, 1)
	go func() {
		if len(r.signals) == 0 {
			// No signal means we don't want to block after each execution on `fn`.
			// We thus close the `signal` so that every `receive` action immediately returns.
			close(signal)
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Will block for one of the signal.
			reflect.Select(r.signals)
			select {
			case signal <- struct{}{}:
			default: // There is already an unconsumed signal in here.
			}
		}
	}()

	// Function responsible for executing `fn` at the right moments.
	go func() {
		defer cancel()
		for {
			log := log.WithField("routine_name", r.name)
			if err := fn(ctx); err != nil {
				if errors.Is(err, &PermanentError{}) {
					log.WithContext(ctx).Panicf("routine %s: %v", r.name, err)
				}
				log.WithContext(ctx).Errorf("routine %s: %v", r.name, err)
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
				log.Infof("routine %s: context cancelled", r.name)
				go func() { <-r.close }()
				r.Close()
				return
			case <-r.close:
				log.Infof("routine %s: Close called", r.name)
				return
			case <-signal:
				log.Debugf("routine %s: received signal", r.name)
			case <-r.retryChannel:
				log.Debugf("routine %s: retrying", r.name)
			}
		}
	}()
	return r
}

// Close closes this routine. It is a blocking call guaranteeing that the routine has exited its loop by the time it returns.
func (r *Routine) Close() {
	r.closeOnce.Do(func() {
		log.Infof("routine %s: send close signal", r.name)
		r.close <- struct{}{}
		if r.ticker != nil {
			r.ticker.Stop()
		}
	})
}

func (r *Routine) execute(ctx context.Context) error {
	if r.timeoutSeconds > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, time.Duration(r.timeoutSeconds)*time.Second)
		defer cancel()
	}
	var err error
	if r.durationHistogram != nil {
		start := time.Now()
		defer func() {
			r.durationHistogram.With(map[string]string{"success": fmt.Sprintf("%v", err == nil)}).Observe(time.Now().Sub(start).Seconds())
		}()
	}
	err = r.fn(ctx)
	if r.errorCounter != nil && err != nil {
		r.errorCounter.Inc()
	}
	return err
}

// CloseInParallelFN returns a function that will close routine in parallel and block until all routines have exited their loop.
func CloseInParallelFN(routines []*Routine) func() {
	log.Infof("closing %d routines in parallel", len(routines))
	return func() {
		wg := sync.WaitGroup{}
		wg.Add(len(routines))
		for _, r := range routines {
			r := r
			go func() { r.Close(); wg.Done() }()
		}
		wg.Wait()
	}
}
