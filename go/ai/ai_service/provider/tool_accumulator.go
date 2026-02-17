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
	"github.com/malonaz/core/go/grpc"
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
		return nil, grpc.Errorf(codes.Internal, "tool call with index %d not found", index).Err()
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
			return nil, grpc.Errorf(codes.Internal, "marshaling structured tool call arguments: %v", err).Err()
		}
	} else {
		lexer := streamingjson.NewLexer()
		lexer.AppendString(entry.args.String())
		healed := lexer.CompleteJSON()
		if healed == "" {
			healed = "{}"
		}
		if err := tc.Arguments.UnmarshalJSON([]byte(healed)); err != nil {
			return nil, grpc.Errorf(codes.Internal, "unmarshaling healed tool call arguments").
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
		return nil, grpc.Errorf(codes.Internal, "tool call with index %d not found", index).Err()
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
			return nil, grpc.Errorf(codes.Internal, "marshaling structured tool call arguments: %v", err).Err()
		}
	} else {
		rawJSON := entry.args.String()
		if err := tc.Arguments.UnmarshalJSON([]byte(rawJSON)); err != nil {
			return nil, grpc.Errorf(codes.Internal, "unmarshaling tool call arguments").
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
