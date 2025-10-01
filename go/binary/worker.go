package binary

import (
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/malonaz/core/go/logging"
)

// Worker manages the lifecycle of a set of binaries, abstracting away the complexities
// of shutting down subprocesses, as well as collecting any unexpected errors these subprocesses encounter.
type Worker struct {
	// Name of this worker, which will be used by the logger.
	name string
	// Allows a client to pass a custom logger to this worker.
	logger *logging.Logger
	// indicates whether this worker was run in sequential mode.
	sequentialMode bool
	// The binaries managed by this worker.
	binaries []*Binary
	// Protects the errors, ensuring that we collect any binary error.
	errorsMutex sync.Mutex
	// Collects any errors encountered by a binary asynchronously.
	errors *multierror.Error
	// Contains the errors callbacks.
	errorCallbacks []func(error)
	// Indicates that this worker is currently terminating.
	terminating bool
	// Ensures that this worker attempt to terminate a single time.
	terminateOnce sync.Once
}

// NewWorker returns a new worker.
func NewWorker(name string, binaries []*Binary) *Worker {
	return &Worker{
		name:     name,
		binaries: binaries,
	}
}

// WithEnv sets the given environment for each of its binary.
func (w *Worker) WithEnv(keyToValue map[string]string) *Worker {
	for key, value := range keyToValue {
		for _, bin := range w.binaries {
			bin.WithEnv(key, value)
		}
	}
	return w
}

// SetLogger sets this worker's logger.
func (w *Worker) SetLogger(logger *logging.Logger) *Worker {
	w.logger = logger
	for _, binary := range w.binaries {
		binary.SetLogger(logger)
	}
	return w
}

// OnError calls the given callback if this worker shuts down unexpected.
// Non-blocking call.
func (w *Worker) OnError(callback func(error)) *Worker {
	w.errorCallbacks = append(w.errorCallbacks, callback)
	return w
}

// GetError returns any error collected from binaries into a single error.
func (w *Worker) GetError() error {
	w.errorsMutex.Lock()
	defer w.errorsMutex.Unlock()
	return w.errors.ErrorOrNil()
}

// Run runs this worker, calling Run() on its binaries in parallel.
// If a binary encounters an error, all binaries will be terminated.
func (w *Worker) Run() {
	if w.logger == nil {
		w.logger = logging.NewRawLogger()
	}
	// Start binaries concurrently.
	wg := sync.WaitGroup{}
	wg.Add(len(w.binaries))
	for _, binary := range w.binaries {
		binary := binary
		go func() { w.runBinary(binary); wg.Done() }()
	}
	wg.Wait()
}

// RunSequentially runs this worker, calling Run() on its binaries sequentially.
// If a binary encounters an error, all binaries will be cleaned up cleanly.
func (w *Worker) RunSequentially() {
	if w.logger == nil {
		w.logger = logging.NewRawLogger()
	}
	w.sequentialMode = true
	for _, binary := range w.binaries {
		w.runBinary(binary)
	}
}

func (w *Worker) runBinary(binary *Binary) {
	// Always die on binary error.
	binary.OnError(func(err error) {
		err = fmt.Errorf("[%s] encountered a fatal error: %w", binary.Name(), err)
		w.errorsMutex.Lock()
		w.errors = multierror.Append(w.errors, err)
		w.errorsMutex.Unlock()
		if !w.terminating {
			w.die(err)
		}
	})
	// Die on exit for non-job binary, unless Exit has been called by this worker,
	// as indicated by the `terminating` field.
	if !binary.IsJob() {
		binary.OnExit(func() {
			if !w.terminating {
				err := fmt.Errorf("[%s] exited unexpectedly", binary.Name())
				w.errorsMutex.Lock()
				w.errors = multierror.Append(w.errors, err)
				w.errorsMutex.Unlock()
				w.die(err)
			}
		})
	}
	binary.Run()
}

func (w *Worker) die(err error) {
	w.terminateOnce.Do(func() {
		for _, errorCallback := range w.errorCallbacks {
			errorCallback(err)
		}
		w.log("dying: %v", err)
		w.terminate()
		w.log("died")
	})
}

// Exit shuts down this worker gracefully.
func (w *Worker) Exit() {
	w.terminateOnce.Do(func() {
		w.log("Exiting gracefully.")
		w.terminate()
		w.log("Exited gracefully.")
	})
}

func (w *Worker) terminate() {
	w.terminating = true
	wg := sync.WaitGroup{}
	wg.Add(len(w.binaries))
	for i := len(w.binaries) - 1; i >= 0; i-- {
		binary := w.binaries[i]
		fn := func() { binary.Exit(); wg.Done() }
		if w.sequentialMode {
			fn()
			continue
		}
		go fn()
	}
	wg.Wait()
}

func (w *Worker) log(msg string, args ...any) {
	newMsg := fmt.Sprintf(msg, args...)
	w.logger.Printf("Worker[%s]: %s.", w.name, newMsg)
}
