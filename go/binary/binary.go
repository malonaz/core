// Package binary offers thin wrappers around running executables.
// The Binary object allows for callbacks OnExit, OnError, allowing
// a caller to monitor the subprocess.
package binary

import (
	"bufio"
	"errors"
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
	// length of time to sleep between attempts to listen to port
	portCheckWaitTime    = 2 * time.Second
	portCheckMaxAttempts = 40
)

// Binary represents an executable binary, or a job.
type Binary struct {
	// Logger allows a caller to pass a custom logger to this binary.
	log *slog.Logger
	// Name of the binary, which will be used by the logger.
	name string
	// Path is the path of the executable. Can be a symbolic link as the library will dereference symbolic links.
	path string
	// Port is the port this binary will open. If a binary does not open ports, it can simply leave this port as `0`.
	port int
	// Cmd holds the subprocess.
	cmd *exec.Cmd
	// Env contains environment variable this binary will execute in.
	env []string
	// Args are used to pass arguments to this binary.
	args []string
	// Job is set to true if this binary is a job.
	// Running a binary in `Job` mode means we will run the binary until it exits.
	job bool

	// Done is used to trigger callbacks on binary exit.
	done chan struct{}
	// Contains the exit callbacks.
	exitCallbacks []func()
	// Contains the errors callbacks.
	errorCallbacks []func(error)
	// Indicates that Exit() has been called.
	exiting bool
	// Ensures we die only once.
	terminateOnce sync.Once
}

// dereferenceLinks dereferences all layers of symbolic links in the input path.
// This is import for things like kafka and zookeeper that look around their own location
// to find other things, but are not smart enough to dereference links themselves.
func dereferenceLinks(path string) (string, error) {
	for {
		if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			path, err = os.Readlink(path)
			if err != nil {
				return "", err
			}
		} else {
			return path, nil
		}
	}
	return path, nil
}

// MustNew instantiates and returns a new binary. Panics if path is invalid.
func MustNew(path string, args ...string) *Binary {
	binary, err := New(path, args...)
	if err != nil {
		panic(err)
	}
	return binary
}

// New returns a new binary.
func New(path string, args ...string) (*Binary, error) {
	realPath, err := lookupPath(path)
	if err != nil {
		return nil, err
	}
	return &Binary{
		log:  slog.Default(),
		name: path,
		path: realPath,
		done: make(chan struct{}),
		args: args,
	}, nil
}

// lookupPath looks up the given filename according to PATH.
// If it contains a path separator then it is assumed to be relative to the current directory, unless
// it's an absolute path.
func lookupPath(filename string) (string, error) {
	if filename[0] == os.PathSeparator {
		// Path is absolute, return it directly.
		return filename, nil
	}
	if strings.ContainsRune(filename, os.PathSeparator) {
		dir, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("could not determine current working directory: %w", err)
		}
		return path.Join(dir, filename), nil
	}
	binaryPath, err := exec.LookPath(filename)
	if err != nil {
		return "", fmt.Errorf("could not look up binary path: %w", err)
	}
	realPath, err := dereferenceLinks(binaryPath)
	if err != nil {
		return "", fmt.Errorf("could not dereference symbolic link: %w", err)
	}
	return realPath, nil
}

// Name returns this binary's name.
func (b *Binary) Name() string {
	return b.name
}

// AsJob flags this binary as a job, which means Run() will wait for program to end.
func (b *Binary) AsJob() *Binary {
	b.job = true
	return b
}

// WithPort sets a port we expect this binary to open.
// If the binary is not a job, `Run` will wait for this port to open before returning.
func (b *Binary) WithPort(port int) *Binary {
	b.port = port
	return b
}

// WithEnv adds a environment variable to this binary, which will be injected into the process when Run() is called.
func (b *Binary) WithEnv(key, value string) *Binary {
	b.env = append(b.env, key+"="+value)
	return b
}

// SetName sets this binary's name.
func (b *Binary) WithName(name string) *Binary {
	b.name = name
	return b
}

// SetLogger sets this binary's logger.
func (b *Binary) WithLogger(logger *slog.Logger) *Binary {
	b.log = logger
	return b
}

// OnError calls the given callback if this binary fails to start or exits with a non-zero status.
// Non-blocking call. For a job, the callbacks are guaranteed to be called before the Run() method terminates.
func (b *Binary) OnError(callback func(error)) *Binary {
	b.errorCallbacks = append(b.errorCallbacks, callback)
	return b
}

// runExitCallbacks runs the exit callbacks.
func (b *Binary) runExitCallbacks() {
	for _, callback := range b.exitCallbacks {
		callback()
	}
}

// runErrorCallbacks runs the error callbacks.
func (b *Binary) runErrorCallbacks(err error) {
	for _, errorCallback := range b.errorCallbacks {
		errorCallback(err)
	}
}

// OnExit calls the given callback when this binary exits.
// Non-blocking call. For a job, the callbacks are guaranteed to be called before the Run() method terminates.
func (b *Binary) OnExit(callback func()) *Binary {
	b.exitCallbacks = append(b.exitCallbacks, callback)
	return b
}

// IsJob returns true if this binary has been flagged as job.
func (b *Binary) IsJob() bool {
	return b.job
}

// RunAsJob is a lean wrapper around `Run` which can be used for jobs (only for jobs)
// to run them to completion or return any error encountered.
func (b *Binary) RunAsJob() error {
	if !b.job {
		return errors.New("Cannot call RunAsJob on a Binary that is not a job")
	}
	var jobError error
	b.OnError(func(err error) { jobError = err })
	b.Run()
	return jobError
}

// Run runs this binary. The call is synchronous if the binary is flagged as a job.
// Run will wait for a port to open if a port is specied.
// If any args are passed to this method, they will override any args defined when creating
// the binary.
func (b *Binary) Run() {
	b.cmd = exec.Command(b.path, b.args...)
	b.cmd.Env = b.env
	if err := b.redirectOutput(b.cmd.StdoutPipe); err != nil {
		b.die(fmt.Errorf("could not listen to stdout pipe: %w", err))
		return
	}
	if err := b.redirectOutput(b.cmd.StderrPipe); err != nil {
		b.die(fmt.Errorf("could not listen to stderr pipe: %w", err))
		return
	}

	if err := b.cmd.Start(); err != nil {
		b.die(fmt.Errorf("could not start process: %w", err))
		return
	}

	go func() {
		// Here we do the following algorithm:
		// - On process exit with non-zero status code && !exiting: run error callbacks.
		// - On process exit with zero status code: run exit callbacks.
		// - Lastly, close the b.done channel.
		defer close(b.done)
		if err := b.cmd.Wait(); err != nil && !b.exiting {
			b.runErrorCallbacks(err)
			return
		}
		b.runExitCallbacks()
	}()

	if b.port != 0 {
		b.waitForPort()
	}

	// If this is a job, we wait for the job to exit.
	if b.job {
		<-b.done
		b.log.Info("job completed")
	}
}

func (b *Binary) redirectOutput(fn func() (io.ReadCloser, error)) error {
	cmdOut, err := fn()
	if err != nil {
		return err
	}
	outScanner := bufio.NewScanner(cmdOut)
	go func() {
		for outScanner.Scan() {
			text := outScanner.Text()
			b.log.Info(text)
		}
	}()
	return nil
}

func (b *Binary) waitForPort() {
	address := fmt.Sprintf("localhost:%d", b.port)
	ticker := time.NewTicker(portCheckWaitTime)
	defer ticker.Stop()
	var conn net.Conn
	var err error
	for range portCheckMaxAttempts {
		<-ticker.C
		conn, err = net.Dial("tcp", address)
		if err != nil {
			continue
		}
		conn.Close()
		return
	}
	b.die(fmt.Errorf("failed to open [%s]'s port [%d]: %w", b.name, b.port, err))
}

func (b *Binary) isRunning() bool {
	return b.cmd != nil && b.cmd.Process != nil && (b.cmd.ProcessState == nil || !b.cmd.ProcessState.Exited())
}

// die terminates this binary gracefully then closes the error channel to signal
// that an inrecuperable error has occurred.
func (b *Binary) die(err error) {
	b.terminateOnce.Do(func() {
		b.log.Error("dying", "error", err)
		if b.isRunning() {
			b.terminate()
		}
		b.log.Error("died")
	})
}

// Exit terminates this binary gracefully.
func (b *Binary) Exit() {
	b.terminateOnce.Do(func() {
		b.exiting = true
		if b.isRunning() {
			b.log.Info("exiting gracefully")
			b.terminate()
			b.log.Info("exited gracefully")
		}
	})
}

func (b *Binary) terminate() {
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		b.log.Error("could not exit process", "error", err)
	}

	// We not wait on done. If you check out the `Run` method, you'll notice that
	// done is closed only when a process exits.
	<-b.done
}
