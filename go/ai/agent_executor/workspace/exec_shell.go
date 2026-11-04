package workspace

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/agent_executor"
)

const (
	execShellToolName = "exec_shell"

	defaultTimeout = 120 * time.Second
	maxTimeout     = 600 * time.Second
	// outputLimit caps the output fed back, split between its head and tail.
	outputLimit = 32 << 10
)

// ExecShellTool runs bash commands in a directory.
type ExecShellTool struct {
	directory string
}

// NewExecShellTool returns a tool running bash commands in the directory.
func NewExecShellTool(directory string) *ExecShellTool {
	return &ExecShellTool{directory: directory}
}

type execShellArguments struct {
	Command          string `json:"command"`
	WorkingDirectory string `json:"working_directory"`
	TimeoutSeconds   int    `json:"timeout_seconds"`
}

func (t *ExecShellTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: execShellToolName,
		Description: "Run a bash command in the workspace and return its exit code and combined stdout and stderr. " +
			"Stdin is empty: never run interactive commands. Long output keeps its head and tail only. " +
			"On timeout, the command and every process it started are killed.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"command": {
					Type:        "string",
					Description: "The bash command to run.",
				},
				"working_directory": {
					Type:        "string",
					Description: "The directory to run in, absolute or relative to the workspace. Defaults to the workspace.",
				},
				"timeout_seconds": {
					Type:        "integer",
					Description: fmt.Sprintf("Seconds before the command is killed. Defaults to %d, at most %d.", int(defaultTimeout.Seconds()), int(maxTimeout.Seconds())),
					Minimum:     1,
					Maximum:     maxTimeout.Seconds(),
				},
			},
			Required: []string{"command"},
		},
	}
}

func (t *ExecShellTool) Execute(ctx context.Context, _ *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &execShellArguments{}
	if err := agent_executor.DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if strings.TrimSpace(arguments.Command) == "" {
		return nil, errors.New("command is required")
	}
	timeout := defaultTimeout
	if arguments.TimeoutSeconds > 0 {
		timeout = min(time.Duration(arguments.TimeoutSeconds)*time.Second, maxTimeout)
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	command := exec.CommandContext(timeoutCtx, "bash", "-c", arguments.Command)
	command.Dir = resolvePath(t.directory, arguments.WorkingDirectory)
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
		return nil, ctx.Err()
	}

	var outcome string
	var exitError *exec.ExitError
	switch {
	case timeoutCtx.Err() != nil:
		outcome = fmt.Sprintf("Timed out after %s: killed.", timeout)
	case err == nil, errors.Is(err, exec.ErrWaitDelay):
		// ErrWaitDelay follows a clean exit: a background process kept the pipe open.
		outcome = "Exit code: 0"
	case errors.As(err, &exitError) && exitError.Exited():
		outcome = fmt.Sprintf("Exit code: %d", exitError.ExitCode())
	case exitError != nil:
		outcome = fmt.Sprintf("Terminated: %v", exitError)
	default:
		return nil, err
	}
	content := outcome + "\n" + output.String()
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
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
