package binary

import (
	"fmt"
	"log/slog"
	"sync"
)

// Worker orchestrates the lifecycle of a group of [Binary] instances,
// handling coordinated startup, error propagation, and shutdown.
//
// In parallel mode ([Worker.Run] or [Worker.Start]), all binaries are
// started concurrently. In sequential mode ([Worker.RunSequentially]),
// binaries are started in order, blocking on each job before proceeding.
//
// If any binary exits unexpectedly or with an error, the worker shuts down
// all remaining binaries and reports the error. Call [Worker.Stop] from
// another goroutine to initiate a graceful shutdown.
type Worker struct {
	log      *slog.Logger
	name     string
	binaries []*Binary

	onExit   func(error)
	stopChan chan struct{}
	mu       sync.Mutex
	stopped  bool
}

// NewWorker returns a new [Worker] managing the given binaries.
func NewWorker(name string, binaries []*Binary) *Worker {
	return &Worker{
		log:      slog.Default(),
		name:     name,
		binaries: binaries,
		stopChan: make(chan struct{}),
	}
}

// WithEnv adds environment variables to every managed binary.
func (w *Worker) WithEnv(keyToValue map[string]string) *Worker {
	for key, value := range keyToValue {
		for _, binary := range w.binaries {
			binary.WithEnv(key, value)
		}
	}
	return w
}

// WithLogger sets the logger for the worker and all managed binaries.
func (w *Worker) WithLogger(logger *slog.Logger) *Worker {
	w.log = logger
	return w
}

// OnExit registers a callback invoked when the worker finishes after
// [Worker.Start]. The error is nil if all binaries exited cleanly or
// [Worker.Stop] was called. Has no effect with [Worker.Run] or
// [Worker.RunSequentially], where the error is returned directly.
func (w *Worker) OnExit(callback func(error)) *Worker {
	w.onExit = callback
	return w
}

// Run starts all binaries concurrently and blocks until one of:
//   - All job binaries complete successfully (services are then stopped).
//   - Any binary exits unexpectedly or with an error.
//   - [Worker.Stop] is called.
//
// Returns the first error encountered, or nil on clean shutdown.
func (w *Worker) Run() error {
	if w.isStopped() {
		return nil
	}
	return w.run()
}

// RunAsync starts all binaries concurrently and returns once every binary
// has launched successfully (and any port checks have passed). If any
// binary fails to start, already-started binaries are shut down and
// the error is returned. After Start returns nil, the worker runs in
// the background; the [Worker.OnExit] callback is invoked when the
// worker finishes. Use [Worker.Stop] for graceful shutdown.
func (w *Worker) RunAsync() error {
	if w.isStopped() {
		return nil
	}

	errChan := make(chan error, len(w.binaries))
	var jobWg sync.WaitGroup

	for _, binary := range w.binaries {
		b := binary
		b.WithLogger(w.log.With("binary", b.Name()))
		if b.IsJob() {
			jobWg.Add(1)
		}
		b.OnExit(w.onBinaryExit(b, errChan, &jobWg))
		if err := b.RunAsync(); err != nil {
			w.shutdown()
			return fmt.Errorf("[%s] failed to start: %w", b.Name(), err)
		}
	}

	go func() {
		err := w.awaitCompletion(errChan, &jobWg)
		w.shutdown()
		if w.onExit != nil {
			w.onExit(err)
		}
	}()
	return nil
}

// RunSequentially starts binaries in declaration order. Job binaries are
// run synchronously via [Binary.Run]; service binaries are started in the
// background via [Binary.Start]. After all binaries have been processed:
//   - If no services are running, returns immediately.
//   - Otherwise, blocks until a service fails or [Worker.Stop] is called.
//
// Returns the first error encountered, or nil on clean shutdown.
func (w *Worker) RunSequentially() error {
	if w.isStopped() {
		return nil
	}
	w.log = w.log.With("worker", w.name, "mode", "sequential")

	errChan := make(chan error, len(w.binaries))
	hasServices := false

	for _, binary := range w.binaries {
		b := binary
		b.WithLogger(w.log.With("binary", b.Name()))
		if b.IsJob() {
			if err := b.Run(); err != nil {
				w.shutdown()
				return fmt.Errorf("[%s]: %w", b.Name(), err)
			}
			continue
		}
		hasServices = true
		b.OnExit(w.onBinaryExit(b, errChan, nil))
		if err := b.RunAsync(); err != nil {
			w.shutdown()
			return fmt.Errorf("[%s] failed to start: %w", b.Name(), err)
		}
	}

	if !hasServices {
		return nil
	}

	var runErr error
	select {
	case err := <-errChan:
		runErr = err
	case <-w.stopChan:
	}

	w.shutdown()
	return runErr
}

// Stop signals the worker to shut down gracefully. The actual shutdown
// completes asynchronously when [Worker.Run] or [Worker.RunSequentially]
// returns, or when the [Worker.OnExit] callback fires after [Worker.Start].
// Safe to call from any goroutine and multiple times.
func (w *Worker) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()
	close(w.stopChan)
}

// run is the shared implementation for [Worker.Run]. It starts all binaries,
// waits for completion, and shuts down.
func (w *Worker) run() error {
	errChan := make(chan error, len(w.binaries))
	var jobWg sync.WaitGroup

	for _, binary := range w.binaries {
		b := binary
		b.WithLogger(w.log.With("binary", b.Name()))
		if b.IsJob() {
			jobWg.Add(1)
		}
		b.OnExit(w.onBinaryExit(b, errChan, &jobWg))
		if err := b.RunAsync(); err != nil {
			w.shutdown()
			return fmt.Errorf("[%s] failed to start: %w", b.Name(), err)
		}
	}

	runErr := w.awaitCompletion(errChan, &jobWg)
	w.shutdown()
	return runErr
}

// awaitCompletion blocks until an error occurs, all jobs finish, or
// [Worker.Stop] is called. Returns the error or nil.
func (w *Worker) awaitCompletion(errChan <-chan error, jobWg *sync.WaitGroup) error {
	hasJobs := false
	for _, b := range w.binaries {
		if b.IsJob() {
			hasJobs = true
			break
		}
	}

	jobsDone := make(chan struct{})
	go func() { jobWg.Wait(); close(jobsDone) }()

	if hasJobs {
		select {
		case err := <-errChan:
			return err
		case <-jobsDone:
			return nil
		case <-w.stopChan:
			return nil
		}
	}

	select {
	case err := <-errChan:
		return err
	case <-w.stopChan:
		return nil
	}
}

// onBinaryExit returns an [Binary.OnExit] callback for a managed binary.
// It ignores exits after the worker has been stopped. For errors or
// unexpected service exits, it sends to errChan. For job completion in
// parallel mode, it decrements jobWg. jobWg may be nil in sequential mode
// where jobs are run synchronously via [Binary.Run].
func (w *Worker) onBinaryExit(b *Binary, errChan chan<- error, jobWg *sync.WaitGroup) func(error) {
	return func(err error) {
		if w.isStopped() {
			return
		}
		if err != nil {
			errChan <- fmt.Errorf("[%s] errored: %w", b.Name(), err)
			return
		}
		if b.IsJob() && jobWg != nil {
			jobWg.Done()
			return
		}
		errChan <- fmt.Errorf("[%s] exited unexpectedly", b.Name())
	}
}

// isStopped reports whether [Worker.Stop] has been called.
func (w *Worker) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

// shutdown stops all managed binaries in reverse declaration order. It is
// idempotent: the first call performs the shutdown, subsequent calls are
// no-ops. It also marks the worker as stopped so that any pending
// [Binary.OnExit] callbacks are suppressed.
func (w *Worker) shutdown() {
	w.mu.Lock()
	alreadyStopped := w.stopped
	w.stopped = true
	w.mu.Unlock()
	if !alreadyStopped {
		close(w.stopChan)
	}
	for i := len(w.binaries) - 1; i >= 0; i-- {
		w.binaries[i].Stop()
	}
}
