package workspace_service

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	defaultTimeout = 120 * time.Second
	// outputLimit caps the output returned, split between its head and tail.
	outputLimit = 32 << 10
)

// Exec implements the interface.
func (s *Service) Exec(ctx context.Context, request *pb.ExecRequest) (*pb.ExecResponse, error) {
	timeout := defaultTimeout
	if request.GetTimeoutSeconds() > 0 {
		timeout = time.Duration(request.GetTimeoutSeconds()) * time.Second
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	command := exec.CommandContext(timeoutCtx, "bash", "-c", request.GetCommand())
	command.Dir = s.resolvePath(request.GetWorkingDirectory())
	// Its own process group, so a kill reaches every process the command started.
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	// Stops a background process holding the output pipe from blocking the call.
	command.WaitDelay = time.Second
	output := newCappedBuffer(outputLimit / 2)
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if ctx.Err() != nil {
		return nil, status.FromError(ctx.Err(), "running command").Err()
	}

	var exitError *exec.ExitError
	switch {
	case timeoutCtx.Err() != nil:
		return &pb.ExecResponse{ExitCode: -1, Output: output.String(), TimedOut: true}, nil
	case err == nil, errors.Is(err, exec.ErrWaitDelay):
		// ErrWaitDelay follows a clean exit: a background process kept the pipe open.
		return &pb.ExecResponse{Output: output.String()}, nil
	case errors.As(err, &exitError):
		// ExitCode is -1 when a signal killed the command.
		return &pb.ExecResponse{ExitCode: int32(exitError.ExitCode()), Output: output.String()}, nil
	default:
		return nil, fileError(err)
	}
}

// cappedBuffer keeps the head and tail of what is written to it, so a
// command's output costs bounded memory however much it prints.
type cappedBuffer struct {
	limit int
	head  []byte
	tail  []byte
	total int
}

func newCappedBuffer(limit int) *cappedBuffer {
	return &cappedBuffer{limit: limit}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.total += len(p)
	headSize := min(b.limit-len(b.head), len(p))
	b.head = append(b.head, p[:headSize]...)
	b.tail = append(b.tail, p[headSize:]...)
	// Trimmed only once it doubled, so trimming copies amortize.
	if len(b.tail) > 2*b.limit {
		b.tail = append(b.tail[:0], b.tail[len(b.tail)-b.limit:]...)
	}
	return len(p), nil
}

// String returns the output kept, marking what was dropped. Proto strings must
// be UTF-8, so invalid bytes are replaced.
func (b *cappedBuffer) String() string {
	tail := b.tail
	if len(tail) > b.limit {
		tail = tail[len(tail)-b.limit:]
	}
	output := string(b.head)
	if truncated := b.total - len(b.head) - len(tail); truncated > 0 {
		output += fmt.Sprintf("\n[... %d bytes truncated ...]\n", truncated)
	}
	return strings.ToValidUTF8(output+string(tail), "\uFFFD")
}
