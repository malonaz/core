package provider_test

import (
	"testing"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	. "github.com/malonaz/core/go/ai/ai_service/provider"
)

func buildToolCall(t *testing.T, a *ToolCallAccumulator, index int64) *aipb.ToolCall {
	t.Helper()
	block, err := a.Build(index)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return block.GetToolCall()
}

// Vertex partial-args streams announce containers as value-less PartialArgs
// (decoded as ""). They must not clobber structure built at the same path.
func TestAppendArgContainerAnnouncementDoesNotClobber(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "Generate_Table")
	a.AppendArg(0, "$.rows[0].cells[0].value", "Phone")
	a.AppendArg(0, "$.rows[0].cells[0]", "") // late container announcement
	a.AppendArg(0, "$.rows[0]", "")

	tc := buildToolCall(t, a, 0)
	rows := tc.GetArguments().AsMap()["rows"].([]any)
	cell := rows[0].(map[string]any)["cells"].([]any)[0].(map[string]any)
	if cell["value"] != "Phone" {
		t.Fatalf("cell = %v, want value=Phone", cell)
	}
}

// The final chunk of a partial-args stream carries the complete args map:
// it must replace the per-path accumulation, not be silently discarded.
func TestSetStructuredArgsReplacesAccumulation(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "Generate_Table")
	a.AppendArg(0, "$.rows[0].cells[0]", "") // placeholder skeleton
	if !a.HasStructuredArgs(0) {
		t.Fatal("expected structured args")
	}
	a.SetStructuredArgs(0, map[string]any{
		"rows": []any{map[string]any{"cells": []any{map[string]any{"value": "Phone"}}}},
	})

	tc := buildToolCall(t, a, 0)
	rows := tc.GetArguments().AsMap()["rows"].([]any)
	cell := rows[0].(map[string]any)["cells"].([]any)[0].(map[string]any)
	if cell["value"] != "Phone" {
		t.Fatalf("cell = %v, want value=Phone", cell)
	}
}

func TestStringDeltaAppend(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "Generate_Text")
	a.AppendArg(0, "$.markdown", "")
	a.AppendArg(0, "$.markdown", "Hello ")
	a.AppendArg(0, "$.markdown", "world")
	tc := buildToolCall(t, a, 0)
	if got := tc.GetArguments().AsMap()["markdown"]; got != "Hello world" {
		t.Fatalf("markdown = %v", got)
	}
}

// A tool called with no arguments produces zero argument deltas (e.g.
// Anthropic sends no input_json_delta for a `{}` input): Build must treat the
// empty buffer as the empty object rather than failing to unmarshal.
func TestBuildEmptyArgumentsIsEmptyObject(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "call_1", "Get_Time")
	toolCall := buildToolCall(t, a, 0)
	if toolCall.GetName() != "Get_Time" {
		t.Fatalf("unexpected tool name: %q", toolCall.GetName())
	}
	if fields := toolCall.GetArguments().GetFields(); len(fields) != 0 {
		t.Fatalf("expected empty arguments, got: %v", fields)
	}
}
