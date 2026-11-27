// Package workspace_service runs commands in one directory: the workspace.
package workspace_service

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/go/grpc/status"
)

type Opts struct {
	Directory    string `long:"directory" env:"DIRECTORY" required:"true" description:"The workspace: the directory commands run in and relative paths resolve against"`
	RunDirectory string `long:"run-directory" env:"RUN_DIRECTORY" default:"/run/workspace" description:"Where SetSecrets writes the secrets and the env file every shell sources"`
}

type runtime struct {
	directory    string
	runDirectory string
	// secretsMutex serializes SetSecrets calls.
	secretsMutex sync.Mutex
}

func newRuntime(opts *Opts) (*runtime, error) {
	directory, err := filepath.Abs(opts.Directory)
	if err != nil {
		return nil, fmt.Errorf("resolving directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("opening directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directory)
	}
	return &runtime{directory: directory, runDirectory: opts.RunDirectory}, nil
}

func (s *Service) start(context.Context) (func(), error) {
	return func() {}, nil
}

// resolvePath resolves a path against the workspace, unless absolute. It
// confines nothing: a caller holding a shell can reach anywhere anyway.
func (r *runtime) resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(r.directory, path)
}

// fileError returns a filesystem error as the gRPC status its cause maps to.
func fileError(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, fs.ErrNotExist):
		code = codes.NotFound
	case errors.Is(err, fs.ErrPermission):
		code = codes.PermissionDenied
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		code = codes.FailedPrecondition
	}
	return status.Errorf(code, "%v", err).Err()
}
