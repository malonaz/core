package provider

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	streamingjson "github.com/karminski/streaming-json-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

type ToolCallAccumulator struct {
	toolCallIDSeed int64
	calls          map[int64]*toolCallEntry
}

type toolCallEntry struct {
	id             string
	name           string
	args           strings.Builder
	structuredArgs map[string]any
	complete       bool
}

func NewToolCallAccumulator() *ToolCallAccumulator {
	return &ToolCallAccumulator{
		toolCallIDSeed: time.Now().UnixNano(),
		calls:          make(map[int64]*toolCallEntry),
	}
}

func (a *ToolCallAccumulator) Has(index int64) bool {
	_, ok := a.calls[index]
	return ok
}

// IndexOf returns the index of the in-flight entry with the given call id —
// the authoritative routing key when the provider supplies one.
func (a *ToolCallAccumulator) IndexOf(id string) (int64, bool) {
	if id == "" {
		return 0, false
	}
	for index, entry := range a.calls {
		if entry.id == id {
			return index, true
		}
	}
	return 0, false
}

// Matches reports whether the entry at index could belong to the given call
// identity. Empty values match on either side: providers often omit the
// id/name on continuation chunks.
func (a *ToolCallAccumulator) Matches(index int64, id, name string) bool {
	entry, ok := a.calls[index]
	if !ok {
		return false
	}
	if id != "" && entry.id != "" && id != entry.id {
		return false
	}
	if name != "" && entry.name != "" && name != entry.name {
		return false
	}
	return true
}

func (a *ToolCallAccumulator) Start(index int64, id, name string) {
	a.calls[index] = &toolCallEntry{id: id, name: name}
}

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

// SetStructuredArgs replaces the entry's accumulated arguments with an
// authoritative full snapshot (e.g. Vertex's complete `args` map on the final
// chunk of a partial-args stream). The incrementally accumulated structured
// args may contain placeholder announcements (empty strings where objects
// belong), so the snapshot must win.
func (a *ToolCallAccumulator) SetStructuredArgs(index int64, args map[string]any) {
	entry, ok := a.calls[index]
	if !ok {
		return
	}
	entry.structuredArgs = args
	entry.args.Reset()
}

// HasStructuredArgs reports whether the entry accumulated per-path structured
// arguments (partial-args mode) rather than raw JSON deltas.
func (a *ToolCallAccumulator) HasStructuredArgs(index int64) bool {
	entry, ok := a.calls[index]
	return ok && entry.structuredArgs != nil
}

func (a *ToolCallAccumulator) AppendArgs(index int64, args string) {
	for idx, entry := range a.calls {
		if idx != index {
			entry.complete = true
		}
	}
	if entry, ok := a.calls[index]; ok {
		if args != "" {
			entry.args.WriteString(args)
		}
	}
}

func (a *ToolCallAccumulator) AppendArg(index int64, jsonPath string, value any) {
	for idx, entry := range a.calls {
		if idx != index {
			entry.complete = true
		}
	}
	entry, ok := a.calls[index]
	if !ok {
		return
	}
	if entry.structuredArgs == nil {
		entry.structuredArgs = make(map[string]any)
	}
	setJSONPath(entry.structuredArgs, jsonPath, value)
}

func (a *ToolCallAccumulator) BuildPartial(index int64) (*aipb.Block, error) {
	entry, ok := a.calls[index]
	if !ok {
		return nil, status.Errorf(codes.Internal, "tool call with index %d not found", index).Err()
	}
	tc := &aipb.ToolCall{
		Id:        entry.id,
		Name:      entry.name,
		Arguments: &structpb.Struct{},
		Partial:   true,
	}
	if tc.Id == "" {
		tc.Id = fmt.Sprintf("call_%s_%d_%d", tc.Name, a.toolCallIDSeed, index)
	}

	if entry.structuredArgs != nil {
		var err error
		tc.Arguments, err = structpb.NewStruct(entry.structuredArgs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling structured tool call arguments: %v", err).Err()
		}
	} else {
		lexer := streamingjson.NewLexer()
		// Best-effort: on a parse error the lexer still holds the prefix that
		// parsed, and healing that is preferable to killing a cosmetic
		// partial. The error is only surfaced if the healed output is unusable.
		appendErr := lexer.AppendString(entry.args.String())
		healed := lexer.CompleteJSON()
		if healed == "" {
			healed = "{}"
		}
		if err := tc.Arguments.UnmarshalJSON([]byte(healed)); err != nil {
			if appendErr != nil {
				return nil, status.Errorf(codes.Internal, "parsing partial tool call arguments: %v", appendErr).
					WithErrorInfo(ai.ErrorInfoReasonToolCallArgumentUnmarshal, "toolAccumulator", map[string]string{"rawJson": healed}).Err()
			}
			return nil, status.Errorf(codes.Internal, "unmarshaling healed tool call arguments").
				WithErrorInfo(ai.ErrorInfoReasonToolCallArgumentUnmarshal, "toolAccumulator", map[string]string{"rawJson": healed}).Err()
		}
	}

	return &aipb.Block{
		Index: index,
		Content: &aipb.Block_PartialToolCall{
			PartialToolCall: tc,
		},
	}, nil
}

func (a *ToolCallAccumulator) Build(index int64) (*aipb.Block, error) {
	entry, ok := a.calls[index]
	if !ok {
		return nil, status.Errorf(codes.Internal, "tool call with index %d not found", index).Err()
	}
	tc := &aipb.ToolCall{
		Id:        entry.id,
		Name:      entry.name,
		Arguments: &structpb.Struct{},
	}
	if tc.Id == "" {
		tc.Id = fmt.Sprintf("call_%s_%d_%d", tc.Name, a.toolCallIDSeed, index)
	}

	if entry.structuredArgs != nil {
		var err error
		tc.Arguments, err = structpb.NewStruct(entry.structuredArgs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling structured tool call arguments: %v", err).Err()
		}
	} else {
		rawJSON := entry.args.String()
		if err := tc.Arguments.UnmarshalJSON([]byte(rawJSON)); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshaling tool call arguments").
				WithErrorInfo(ai.ErrorInfoReasonToolCallArgumentUnmarshal, "toolAccumulator", map[string]string{"rawJson": rawJSON}).Err()
		}
	}

	delete(a.calls, index)
	return &aipb.Block{
		Index: index,
		Content: &aipb.Block_ToolCall{
			ToolCall: tc,
		},
	}, nil
}

func (a *ToolCallAccumulator) BuildComplete() ([]*aipb.Block, error) {
	var blocks []*aipb.Block
	for index, entry := range a.calls {
		if !entry.complete {
			continue
		}
		block, err := a.Build(index)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (a *ToolCallAccumulator) BuildRemaining() ([]*aipb.Block, error) {
	var blocks []*aipb.Block
	for index := range a.calls {
		block, err := a.Build(index)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func setJSONPath(root map[string]any, jsonPath string, value any) {
	segments := parseJSONPathSegments(jsonPath)
	if len(segments) == 0 {
		return
	}
	setValueAtPath(root, segments, value)
}

func setValueAtPath(node map[string]any, segments []any, value any) {
	key, ok := segments[0].(string)
	if !ok {
		return
	}

	if len(segments) == 1 {
		if existing, ok := node[key].(string); ok {
			if strVal, ok := value.(string); ok {
				node[key] = existing + strVal
				return
			}
		}
		// A value-less PartialArg (container announcement) decodes as "".
		// Never let it clobber structure already built at this path.
		if value == "" {
			if _, ok := node[key].(map[string]any); ok {
				return
			}
			if _, ok := node[key].([]any); ok {
				return
			}
		}
		node[key] = value
		return
	}

	switch nextSegment := segments[1].(type) {
	case string:
		child, ok := node[key].(map[string]any)
		if !ok {
			child = make(map[string]any)
			node[key] = child
		}
		setValueAtPath(child, segments[1:], value)

	case int:
		arr, ok := node[key].([]any)
		if !ok {
			arr = make([]any, 0)
		}
		for len(arr) <= nextSegment {
			arr = append(arr, nil)
		}
		node[key] = arr

		if len(segments) == 2 {
			if existing, ok := arr[nextSegment].(string); ok {
				if strVal, ok := value.(string); ok {
					arr[nextSegment] = existing + strVal
					return
				}
			}
			// Container announcement (see above): keep the built structure.
			if value == "" {
				if _, ok := arr[nextSegment].(map[string]any); ok {
					return
				}
				if _, ok := arr[nextSegment].([]any); ok {
					return
				}
			}
			arr[nextSegment] = value
			return
		}

		child, ok := arr[nextSegment].(map[string]any)
		if !ok {
			child = make(map[string]any)
			arr[nextSegment] = child
		}
		setValueAtPath(child, segments[2:], value)
	}
}

func parseJSONPathSegments(path string) []any {
	path = strings.TrimPrefix(path, "$")
	var segments []any
	i := 0
	for i < len(path) {
		if path[i] == '.' {
			i++
			j := i
			for j < len(path) && path[j] != '.' && path[j] != '[' {
				j++
			}
			if j > i {
				segments = append(segments, path[i:j])
			}
			i = j
		} else if path[i] == '[' {
			i++
			j := i
			for j < len(path) && path[j] != ']' {
				j++
			}
			idx, _ := strconv.Atoi(path[i:j])
			segments = append(segments, idx)
			i = j + 1
		} else {
			i++
		}
	}
	return segments
}
