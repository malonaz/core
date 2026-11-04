package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/agent_executor"
)

const (
	readFilesToolName = "read_files"

	// maxFileSize bounds the files read whole into memory.
	maxFileSize = 8 << 20
	// maxReadSize caps the content fed back per file.
	maxReadSize      = 128 << 10
	defaultLineLimit = 2000
)

// ReadFilesTool reads text files.
type ReadFilesTool struct {
	directory string
}

// NewReadFilesTool returns a tool reading text files, resolving relative paths
// against the directory.
func NewReadFilesTool(directory string) *ReadFilesTool {
	return &ReadFilesTool{directory: directory}
}

type readFilesArguments struct {
	Paths  []string `json:"paths"`
	Offset int      `json:"offset"`
	Limit  int      `json:"limit"`
}

func (t *ReadFilesTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: readFilesToolName,
		Description: "Read one or more text files, each under a `==> path <==` header. " +
			fmt.Sprintf("Each file returns at most %d lines (%d KiB); a footer then gives the offset to read on from. ", defaultLineLimit, maxReadSize>>10) +
			"A file that cannot be read reports its error in place of its content.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"paths": {
					Type:        "array",
					Description: "The files to read, absolute or relative to the workspace.",
					MinItems:    1,
					Items:       &jsonpb.Schema{Type: "string"},
				},
				"offset": {
					Type:        "integer",
					Description: "The 1-based line to start reading at, in every file. Defaults to 1.",
					Minimum:     1,
				},
				"limit": {
					Type:        "integer",
					Description: fmt.Sprintf("The most lines to read per file. Defaults to and at most %d.", defaultLineLimit),
					Minimum:     1,
					Maximum:     defaultLineLimit,
				},
			},
			Required: []string{"paths"},
		},
	}
}

func (t *ReadFilesTool) Execute(_ context.Context, _ *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &readFilesArguments{}
	if err := agent_executor.DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if len(arguments.Paths) == 0 {
		return nil, errors.New("paths is required")
	}
	offset := max(arguments.Offset, 1)
	limit := defaultLineLimit
	if arguments.Limit > 0 {
		limit = min(arguments.Limit, defaultLineLimit)
	}
	var builder strings.Builder
	for i, path := range arguments.Paths {
		if i > 0 {
			builder.WriteString("\n")
		}
		fmt.Fprintf(&builder, "==> %s <==\n", path)
		// One unreadable file never sinks the others.
		content, err := readFile(resolvePath(t.directory, path), offset, limit)
		if err != nil {
			fmt.Fprintf(&builder, "Error: %v\n", err)
			continue
		}
		builder.WriteString(content)
	}
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), builder.String()), nil
}

// readFile returns a window of a text file's lines, with a footer locating it
// whenever it is not the whole file.
func readFile(path string, offset, limit int) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return "", errors.New("is a directory")
	}
	if info.Size() > maxFileSize {
		return "", fmt.Errorf("%d bytes, over the %d byte limit: read parts of it with exec_shell", info.Size(), maxFileSize)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if bytes.IndexByte(content, 0) != -1 || !utf8.Valid(content) {
		return "", errors.New("not a text file")
	}
	if len(content) == 0 {
		return "(empty file)\n", nil
	}
	lines := strings.SplitAfter(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if offset > len(lines) {
		return "", fmt.Errorf("offset %d is past the end of the file (%d lines)", offset, len(lines))
	}

	start, end := offset-1, min(offset-1+limit, len(lines))
	var builder strings.Builder
	for i := start; i < end; i++ {
		if builder.Len()+len(lines[i]) > maxReadSize {
			if i == start {
				// A single line over the cap still shows its beginning.
				builder.WriteString(strings.ToValidUTF8(lines[i][:maxReadSize], ""))
				i++
			}
			end = i
			break
		}
		builder.WriteString(lines[i])
	}
	if !strings.HasSuffix(builder.String(), "\n") {
		builder.WriteString("\n")
	}
	switch {
	case end < len(lines):
		fmt.Fprintf(&builder, "[lines %d-%d of %d: read on with offset %d]\n", start+1, end, len(lines), end+1)
	case start > 0:
		fmt.Fprintf(&builder, "[lines %d-%d of %d]\n", start+1, end, len(lines))
	}
	return builder.String(), nil
}
