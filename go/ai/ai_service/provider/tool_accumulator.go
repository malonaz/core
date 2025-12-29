package provider

import (
	"fmt"
	"strings"

	streamingjson "github.com/karminski/streaming-json-go"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// ToolCallAccumulator accumulates streaming tool call chunks for multiple tool calls.
type ToolCallAccumulator struct {
	calls map[int64]*toolCallEntry
}

type toolCallEntry struct {
	id          string
	name        string
	args        strings.Builder
	complete    bool
	extraFields *structpb.Struct
}

func NewToolCallAccumulator() *ToolCallAccumulator {
	return &ToolCallAccumulator{
		calls: make(map[int64]*toolCallEntry),
	}
}

// Has returns whether a tool call exists at the given index.
func (a *ToolCallAccumulator) Has(index int64) bool {
	_, ok := a.calls[index]
	return ok
}

// Start begins accumulating a new tool call at the given index.
func (a *ToolCallAccumulator) Start(index int64, id, name string) {
	a.calls[index] = &toolCallEntry{id: id, name: name}
}

// StartOrUpdate begins or updates a tool call at the given index.
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

// AppendArgs appends to the accumulated arguments and optionally stores metadata.
// Marks all other entries as complete.
func (a *ToolCallAccumulator) AppendArgs(index int64, s string, extraFields *structpb.Struct) {
	for idx, entry := range a.calls {
		if idx != index {
			entry.complete = true
		}
	}
	if entry, ok := a.calls[index]; ok {
		entry.args.WriteString(s)
		if extraFields != nil {
			if entry.extraFields == nil {
				entry.extraFields = &structpb.Struct{}
			}
			proto.Merge(entry.extraFields, extraFields)
		}
	}
}

// BuildPartial returns a partial ToolCall for a given index using streaming JSON healing.
func (a *ToolCallAccumulator) BuildPartial(index int64) (*aipb.ToolCall, error) {
	entry, ok := a.calls[index]
	if !ok {
		return nil, fmt.Errorf("tool call with index %d not found", index)
	}
	tc := &aipb.ToolCall{
		Id:          entry.id,
		Name:        entry.name,
		ExtraFields: entry.extraFields,
	}
	lexer := streamingjson.NewLexer()
	lexer.AppendString(entry.args.String())
	healed := lexer.CompleteJSON()
	if healed == "" {
		healed = "{}"
	}
	var err error
	tc.Arguments, err = pbutil.NewStructFromJSON([]byte(healed))
	if err != nil {
		return nil, err
	}
	return tc, nil
}

// Build returns the completed ToolCall proto for a given index and removes it.
func (a *ToolCallAccumulator) Build(index int64) (*aipb.ToolCall, error) {
	entry, ok := a.calls[index]
	if !ok {
		return nil, fmt.Errorf("tool call with index %d not found", index)
	}
	tc := &aipb.ToolCall{
		Id:          entry.id,
		Name:        entry.name,
		ExtraFields: entry.extraFields,
	}
	var err error
	tc.Arguments, err = pbutil.NewStructFromJSON([]byte(entry.args.String()))
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
