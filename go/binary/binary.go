package binary

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	portCheckInterval    = 2 * time.Second
	portCheckMaxAttempts = 40
)

// Binary represents an executable subprocess with lifecycle management.
//
// A Binary must be created with [New] or [MustNew], configured via builder
// methods, and then executed with either [Binary.Run] (synchronous) or
// [Binary.Start] (asynchronous). Only one of Run/Start may be called per
// Binary instance.
type Binary struct {
	log  *slog.Logger
	name string
	path string
	port int
	env  []string
	args []string
	job  bool

	onExit   func(error)
	cmd      *exec.Cmd
	exitErr  chan error
	done     chan struct{}
	outputWg sync.WaitGroup
	mu       sync.Mutex
	started  bool
	stopped  bool
}

// New returns a new [Binary] for the given executable path and arguments.
// The path is resolved against PATH if it contains no separator, or against
// the current working directory if it is relative.
func New(binaryPath string, args ...string) (*Binary, error) {
	realPath, err := lookupPath(binaryPath)
	if err != nil {
		return nil, err
	}
	return &Binary{
		log:  slog.Default(),
		name: binaryPath,
		path: realPath,
		args: args,
		done: make(chan struct{}),
	}, nil
}

// MustNew is like [New] but panics on error.
func MustNew(binaryPath string, args ...string) *Binary {
	binary, err := New(binaryPath, args...)
	if err != nil {
		panic(err)
	}
	return binary
}

// Name returns this binary's display name.
func (b *Binary) Name() string { return b.name }

// IsJob reports whether this binary has been flagged as a job via [Binary.AsJob].
func (b *Binary) IsJob() bool { return b.job }

// AsJob flags this binary as a finite task rather than a long-running service.
// This is used by [Worker] to determine expected lifecycle behavior: a job is
// expected to exit on its own, while a service exiting is treated as an error.
func (b *Binary) AsJob() *Binary {
	b.job = true
	return b
}

// WithPort sets a TCP port that this binary is expected to open. When using
// [Binary.Start], the call blocks until the port accepts connections or a
// timeout is reached. Has no effect with [Binary.Run].
func (b *Binary) WithPort(port int) *Binary {
	b.port = port
	return b
}

// WithName overrides this binary's display name used in log output.
func (b *Binary) WithName(name string) *Binary {
	b.name = name
	return b
}

// WithLogger sets the logger for this binary's stdout and stderr output.
func (b *Binary) WithLogger(logger *slog.Logger) *Binary {
	b.log = logger
	return b
}

// WithEnv adds an environment variable to this binary's process.
func (b *Binary) WithEnv(key, value string) *Binary {
	b.env = append(b.env, key+"="+value)
	return b
}

// OnExit registers a callback invoked when the process exits after
// [Binary.Start]. The error is nil if the process exited with status 0 or
// was stopped via [Binary.Stop]. Has no effect with [Binary.Run], where the
// exit error is returned directly.
func (b *Binary) OnExit(callback func(error)) *Binary {
	b.onExit = callback
	return b
}

// Run starts the process and blocks until it exits. Returns nil if the
// process exits with status 0, or an error otherwise. For background
// execution, use [Binary.Start] instead.
func (b *Binary) Run() error {
	if err := b.start(); err != nil {
		return err
	}
	return <-b.exitErr
}

// RunAsync starts the process asynchronously. It returns an error if the
// process fails to launch or, when a port is configured via [Binary.WithPort],
// if the port does not accept connections within the timeout. The
// [Binary.OnExit] callback is invoked when the process later exits.
// Use [Binary.Stop] for graceful shutdown.
func (b *Binary) RunAsync() error {
	if err := b.start(); err != nil {
		return err
	}
	go func() {
		err := <-b.exitErr
		if b.onExit != nil {
			b.onExit(err)
		}
	}()
	if b.port != 0 {
		if err := b.waitForPort(); err != nil {
			b.Stop()
			return err
		}
	}
	return nil
}

// Stop sends SIGTERM to the process and waits for it to exit. The
// [Binary.OnExit] callback receives a nil error. Stop is safe to call
// multiple times or on a binary that was never started.
func (b *Binary) Stop() {
	b.mu.Lock()
	if !b.started || b.stopped {
		b.mu.Unlock()
		return
	}
	b.stopped = true
	b.mu.Unlock()

	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		b.log.Error("failed to send SIGTERM", "binary", b.name, "error", err)
	}
	<-b.done
}

// start creates and launches the subprocess. It wires up output piping and
// spawns a goroutine that waits for process exit, drains output, and sends
// the result to exitErr before closing done.
func (b *Binary) start() error {
	b.cmd = exec.Command(b.path, b.args...)
	b.cmd.Env = b.env
	if err := b.pipeOutput(b.cmd.StdoutPipe); err != nil {
		return fmt.Errorf("stdout pipe for %s: %w", b.name, err)
	}
	if err := b.pipeOutput(b.cmd.StderrPipe); err != nil {
		return fmt.Errorf("stderr pipe for %s: %w", b.name, err)
	}
	if err := b.cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", b.name, err)
	}

	b.mu.Lock()
	b.started = true
	b.mu.Unlock()

	b.exitErr = make(chan error, 1)
	go func() {
		defer close(b.done)
		waitErr := b.cmd.Wait()
		b.outputWg.Wait()

		b.mu.Lock()
		stopped := b.stopped
		b.mu.Unlock()

		if stopped {
			b.exitErr <- nil
		} else {
			b.exitErr <- waitErr
		}
	}()
	return nil
}

// pipeOutput connects a command's output pipe to the binary's logger. Each
// line is logged at Info level. The outputWg is used to ensure all output
// is drained before the exit result is published.
func (b *Binary) pipeOutput(fn func() (io.ReadCloser, error)) error {
	reader, err := fn()
	if err != nil {
		return err
	}
	b.outputWg.Add(1)
	go func() {
		defer b.outputWg.Done()
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			b.log.Info(scanner.Text())
		}
	}()
	return nil
}

// waitForPort polls a TCP address until it accepts a connection. It also
// monitors b.done so that an early process exit is detected immediately
// rather than waiting for the full timeout.
func (b *Binary) waitForPort() error {
	address := fmt.Sprintf("localhost:%d", b.port)
	ticker := time.NewTicker(portCheckInterval)
	defer ticker.Stop()
	var lastErr error
	for range portCheckMaxAttempts {
		select {
		case <-b.done:
			return fmt.Errorf("%s exited before port %d opened", b.name, b.port)
		case <-ticker.C:
		}
		conn, err := net.Dial("tcp", address)
		if err != nil {
			lastErr = err
			continue
		}
		conn.Close()
		return nil
	}
	return fmt.Errorf("port %d for %s did not open: %w", b.port, b.name, lastErr)
}

// dereferenceLinks resolves all layers of symbolic links at the given path.
func dereferenceLinks(linkPath string) (string, error) {
	for {
		fi, err := os.Lstat(linkPath)
		if err != nil {
			return linkPath, nil
		}
		if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
			return linkPath, nil
		}
		linkPath, err = os.Readlink(linkPath)
		if err != nil {
			return "", err
		}
	}
}

// lookupPath resolves an executable filename to an absolute path.
// Absolute paths are returned as-is. Relative paths containing a separator
// are resolved against the working directory. Bare names are looked up in
// PATH and have symbolic links dereferenced.
func lookupPath(filename string) (string, error) {
	if filename[0] == os.PathSeparator {
		return filename, nil
	}
	if strings.ContainsRune(filename, os.PathSeparator) {
		dir, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("determine cwd: %w", err)
		}
		return path.Join(dir, filename), nil
	}
	binaryPath, err := exec.LookPath(filename)
	if err != nil {
		return "", fmt.Errorf("look up %s in PATH: %w", filename, err)
	}
	return dereferenceLinks(binaryPath)
}
