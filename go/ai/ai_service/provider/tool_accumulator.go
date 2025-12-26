package provider

import (
	"fmt"
	"strings"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
)

// ToolCallAccumulator accumulates streaming tool call chunks for multiple tool calls.
type ToolCallAccumulator struct {
	calls map[int64]*toolCallEntry
}

type toolCallEntry struct {
	id       string
	name     string
	args     strings.Builder
	complete bool
}

func NewToolCallAccumulator() *ToolCallAccumulator {
	return &ToolCallAccumulator{
		calls: make(map[int64]*toolCallEntry),
	}
}

// Start begins accumulating a new tool call at the given index.
func (a *ToolCallAccumulator) Start(index int64, id, name string) {
	a.calls[index] = &toolCallEntry{id: id, name: name}
}

// Start begins accumulating a new tool call at the given index.
func (a *ToolCallAccumulator) StartOrUpdate(index int64, id, name string) {
	entry, ok := a.calls[index]
	if !ok {
		entry = &toolCallEntry{id: id, name: name}
		a.calls[index] = entry
	}
	if id != "" {
		entry.id = id
	}
	if name != "" {
		entry.name = name
	}
}

// AppendArgs appends to the accumulated arguments for a tool call and marks all other entries as complete.
func (a *ToolCallAccumulator) AppendArgs(index int64, s string) {
	for idx, entry := range a.calls {
		if idx != index {
			entry.complete = true
		}
	}
	if entry, ok := a.calls[index]; ok {
		entry.args.WriteString(s)
	}
}

// Build returns the completed ToolCall proto for a given index and removes it.
func (a *ToolCallAccumulator) Build(index int64) (*aipb.ToolCall, error) {
	entry, ok := a.calls[index]
	if !ok {
		return nil, fmt.Errorf("tool call with index %d not found", index)
	}
	tc := &aipb.ToolCall{
		Id:   entry.id,
		Name: entry.name,
	}
	var err error
	tc.Arguments, err = pbutil.JSONUnmarshalStruct([]byte(entry.args.String()))
	if err != nil {
		return nil, err
	}
	delete(a.calls, index)
	return tc, nil
}

// BuildComplete returns all completed tool calls and removes them from the accumulator.
func (a *ToolCallAccumulator) BuildComplete() ([]*aipb.ToolCall, error) {
	var result []*aipb.ToolCall
	for index, entry := range a.calls {
		if !entry.complete {
			continue
		}
		tc, err := a.Build(index)
		if err != nil {
			return nil, err
		}
		result = append(result, tc)
	}
	return result, nil
}

// BuildRemaining returns all remaining tool calls and removes them from the accumulator.
func (a *ToolCallAccumulator) BuildRemaining() ([]*aipb.ToolCall, error) {
	var result []*aipb.ToolCall
	for index := range a.calls {
		tc, err := a.Build(index)
		if err != nil {
			return nil, err
		}
		result = append(result, tc)
	}
	return result, nil
}
