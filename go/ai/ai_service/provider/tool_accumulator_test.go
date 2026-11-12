package provider_test

import (
	"testing"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
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

// Vertex announces containers with "{}"/"[]" path suffixes ("$.quote{}"):
// the suffix must not leak as a literal key ("quote{}": "") and "$.items[]"
// must not be misread as a write to items[0].
func TestContainerAnnouncementSuffixes(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "call-1", "CreateQuote")
	a.AppendArg(0, "$.quote{}", "")
	a.AppendArg(0, "$.quote.title", "Roofing Estimate")
	a.AppendArg(0, "$.items[]", "")
	a.AppendArg(0, "$.items[0]{}", "")
	a.AppendArg(0, "$.items[0].name", "Tear-off")

	arguments := buildToolCall(t, a, 0).GetArguments().AsMap()
	if _, ok := arguments["quote{}"]; ok {
		t.Fatalf("arguments = %v, want no literal quote{} key", arguments)
	}
	quote := arguments["quote"].(map[string]any)
	if quote["title"] != "Roofing Estimate" {
		t.Fatalf("quote = %v, want title=Roofing Estimate", quote)
	}
	item := arguments["items"].([]any)[0].(map[string]any)
	if item["name"] != "Tear-off" {
		t.Fatalf("items[0] = %v, want name=Tear-off", item)
	}
}

// A late (or repeated) container announcement must not wipe structure
// already built at the announced path.
func TestContainerAnnouncementDoesNotClobberExisting(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "call-1", "Tool")
	a.AppendArg(0, "$.quote.title", "Kept")
	a.AppendArg(0, "$.quote{}", "")
	a.AppendArg(0, "$.items[0].name", "Kept")
	a.AppendArg(0, "$.items[]", "")
	a.AppendArg(0, "$.items[0]{}", "")

	arguments := buildToolCall(t, a, 0).GetArguments().AsMap()
	if arguments["quote"].(map[string]any)["title"] != "Kept" {
		t.Fatalf("quote = %v, want title=Kept", arguments["quote"])
	}
	if arguments["items"].([]any)[0].(map[string]any)["name"] != "Kept" {
		t.Fatalf("items = %v, want [0].name=Kept", arguments["items"])
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

// Vertex quotes JSONPath keys inconsistently ($."rows"[0], $."title"). The
// quotes must never leak into the argument map: a literal `"rows` key is
// unmatchable by downstream consumers (the exact failure seen with
// Generate_Table rendering an empty table).
func TestQuotedJSONPathKeys(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "Generate_Table")
	a.AppendArg(0, `$."title"`, "Financing")
	a.AppendArg(0, `$."rows"[]`, "") // quoted container announcement
	a.AppendArg(0, `$."rows"[0].cells[0].value`, "Will")
	a.AppendArg(0, `$['columns'][0].label`, "Homeowner") // bracketed string key
	a.AppendArg(0, `$."we\"ird"`, "escaped")             // escaped quote inside key

	args := buildToolCall(t, a, 0).GetArguments().AsMap()
	if args["title"] != "Financing" {
		t.Fatalf("title = %v, want Financing (keys: %v)", args["title"], args)
	}
	rows, ok := args["rows"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("rows = %v, want populated array", args["rows"])
	}
	cell := rows[0].(map[string]any)["cells"].([]any)[0].(map[string]any)
	if cell["value"] != "Will" {
		t.Fatalf("cell = %v, want value=Will", cell)
	}
	columns, ok := args["columns"].([]any)
	if !ok || columns[0].(map[string]any)["label"] != "Homeowner" {
		t.Fatalf("columns = %v, want [{label: Homeowner}]", args["columns"])
	}
	if args[`we"ird`] != "escaped" {
		t.Fatalf(`we"ird = %v, want escaped`, args[`we"ird`])
	}
}

// A dangling quote — the container suffix trimmed off a path like
// `$."rows[]` — must still resolve to the bare key.
func TestDanglingQuoteJSONPathKey(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "Generate_Table")
	a.AppendArg(0, `$."rows[]`, "")
	a.AppendArg(0, `$."rows"[0].id`, "r1")

	args := buildToolCall(t, a, 0).GetArguments().AsMap()
	rows, ok := args["rows"].([]any)
	if !ok || rows[0].(map[string]any)["id"] != "r1" {
		t.Fatalf("rows = %v, want [{id: r1}]", args["rows"])
	}
}

// Vertex writes a map key unquoted, dots and all: the paths below are a real
// CreateReport's. Read as nesting, the labels arrive as objects where the
// request wants strings, and the call is refused.
func TestDottedMapKey(t *testing.T) {
	tool := &aipb.Tool{
		Name: "CreateReport",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"report": {
					Type: "object",
					Properties: map[string]*jsonpb.Schema{
						"title":  {Type: "string"},
						"labels": {Type: "object", AdditionalProperties: &jsonpb.Schema{Type: "string"}},
					},
				},
				"revision": {
					Type: "object",
					Properties: map[string]*jsonpb.Schema{
						"queries": {
							Type: "object",
							AdditionalProperties: &jsonpb.Schema{
								Type:       "object",
								Properties: map[string]*jsonpb.Schema{"sql": {Type: "string"}},
							},
						},
					},
				},
			},
		},
	}
	a := NewToolCallAccumulator(tool)
	a.Start(0, "id", "CreateReport")
	a.AppendArg(0, "$.report.labels.analytics.onikisu.com/folder", "payments")
	a.AppendArg(0, "$.report.labels.analytics.onikisu.com/folder", "")
	a.AppendArg(0, "$.report.labels.analytics.onikisu.com/tag.sample", "")
	a.AppendArg(0, "$.report.labels{}", "")
	a.AppendArg(0, "$.report.title", "Payments Overview Sample")
	a.AppendArg(0, "$.report{}", "")
	// A map of messages nests as before.
	a.AppendArg(0, "$.revision.queries.totals.sql", "SELECT 1")

	arguments := buildToolCall(t, a, 0).GetArguments().AsMap()
	report := arguments["report"].(map[string]any)
	wantLabels := map[string]any{
		"analytics.onikisu.com/folder":     "payments",
		"analytics.onikisu.com/tag.sample": "",
	}
	if labels, ok := report["labels"].(map[string]any); !ok || len(labels) != 2 ||
		labels["analytics.onikisu.com/folder"] != wantLabels["analytics.onikisu.com/folder"] ||
		labels["analytics.onikisu.com/tag.sample"] != wantLabels["analytics.onikisu.com/tag.sample"] {
		t.Fatalf("labels = %v, want %v", report["labels"], wantLabels)
	}
	if report["title"] != "Payments Overview Sample" {
		t.Fatalf("title = %v", report["title"])
	}
	queries := arguments["revision"].(map[string]any)["queries"].(map[string]any)
	if queries["totals"].(map[string]any)["sql"] != "SELECT 1" {
		t.Fatalf("queries = %v, want totals.sql", queries)
	}
}

// A call to a tool the accumulator has no schema for keeps the path as written.
func TestDottedMapKeyWithoutSchema(t *testing.T) {
	a := NewToolCallAccumulator()
	a.Start(0, "id", "CreateReport")
	a.AppendArg(0, "$.report.labels.analytics.onikisu.com/folder", "payments")

	labels := buildToolCall(t, a, 0).GetArguments().AsMap()["report"].(map[string]any)["labels"].(map[string]any)
	if _, ok := labels["analytics"].(map[string]any); !ok {
		t.Fatalf("labels = %v, want the path nested as written", labels)
	}
}
