package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider/mock"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil"
)

const (
	toolSetName       = "TestSet"
	discoveryToolName = "TestSet_Discover"

	// The provider-visible tool list: the discovery tool and the
	// pre-discovered tool. It must remain identical across every generation
	// turn to preserve the provider prompt cache.
	expectedProviderTools = discoveryToolName + ",ToolA"
)

// newTool returns a message-generation tool belonging to the test tool set.
func newTool(name string, preDiscovered bool) *aipb.Tool {
	annotations := map[string]string{
		aitool.AnnotationKeyToolType:         aitool.AnnotationValueToolTypeGenerateMessage,
		aitool.AnnotationKeyProtoMessage:     "malonaz.ai.v1.Tool",
		aitool.AnnotationKeyToolSetName:      toolSetName,
		aitool.AnnotationKeyDiscoverableTool: aip.LabelValueTrue,
	}
	if preDiscovered {
		annotations[aitool.AnnotationKeyDiscoverableTool] = aip.LabelValueFalse
		annotations[aitool.AnnotationKeyPreDiscoveredTool] = aip.LabelValueTrue
	}
	return &aipb.Tool{
		Name:        name,
		Description: "Test tool " + name,
		JsonSchema:  &jsonpb.Schema{Type: "object"},
		Annotations: annotations,
	}
}

// newToolSet returns a tool set with ToolA pre-discovered and ToolB & ToolC
// discoverable.
func newToolSet() *aipb.ToolSet {
	toolA := newTool("ToolA", true)
	toolB := newTool("ToolB", false)
	toolC := newTool("ToolC", false)
	createDiscoveryToolRequest := &aitool.CreateDiscoveryToolRequest{
		Name:        discoveryToolName,
		Description: "Discover test tools",
		Tools:       []*aipb.Tool{toolB, toolC},
	}
	discoveryTool := aitool.CreateDiscoveryTool(createDiscoveryToolRequest)
	aip.SetAnnotation(discoveryTool, aitool.AnnotationKeyToolSetName, toolSetName)
	return &aipb.ToolSet{
		Name:          toolSetName,
		DiscoveryTool: discoveryTool,
		Tools:         []*aipb.Tool{toolA, toolB, toolC},
	}
}

// newScriptedUserMessage returns a user message carrying the mock provider
// script: one assistant message replayed per generation turn.
func newScriptedUserMessage(t *testing.T, scriptedMessages ...*aipb.Message) *aipb.Message {
	t.Helper()
	script, err := mock.Script(scriptedMessages...)
	require.NoError(t, err)
	return &aipb.Message{
		Role:        aipb.Role_ROLE_USER,
		Blocks:      []*aipb.Block{{Content: &aipb.Block_Text{Text: "scripted turn"}}},
		Annotations: map[string]string{mock.ScriptAnnotationKey: script},
	}
}

func newAssistantText(text string) *aipb.Message {
	return &aipb.Message{
		Role:   aipb.Role_ROLE_ASSISTANT,
		Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: text}}},
	}
}

func newAssistantToolCall(t *testing.T, id, name string, arguments map[string]any) *aipb.Message {
	t.Helper()
	argumentsStruct, err := structpb.NewStruct(arguments)
	require.NoError(t, err)
	return &aipb.Message{
		Role: aipb.Role_ROLE_ASSISTANT,
		Blocks: []*aipb.Block{{
			Content: &aipb.Block_ToolCall{ToolCall: &aipb.ToolCall{Id: id, Name: name, Arguments: argumentsStruct}},
		}},
	}
}

func newToolResultMessage(toolResult *aipb.ToolResult) *aipb.Message {
	return &aipb.Message{
		Role:   aipb.Role_ROLE_TOOL,
		Blocks: []*aipb.Block{{Content: &aipb.Block_ToolResult{ToolResult: toolResult}}},
	}
}

func generate(t *testing.T, parent string, toolSet *aipb.ToolSet, messages ...*aipb.Message) (*aiservicepb.GenerateMessageResponse, error) {
	t.Helper()
	generateMessageRequest := &aiservicepb.GenerateMessageRequest{
		Parent:   parent,
		Model:    mockModel,
		Messages: messages,
		ToolSets: []*aipb.ToolSet{toolSet},
	}
	return aiServiceClient.GenerateMessage(ctx, generateMessageRequest)
}

func TestGenerateMessageProviderToolListIsStatic(t *testing.T) {
	t.Parallel()
	parent := newChatParent()
	userMessage := newScriptedUserMessage(t, newAssistantText(mock.ToolsPlaceholder))

	generateMessageResponse, err := generate(t, parent, newToolSet(), userMessage)
	require.NoError(t, err)
	require.Equal(t, expectedProviderTools, generateMessageResponse.GetGeneratedMessage().GetBlocks()[0].GetText())
	require.Equal(t, aiservicepb.StopReason_STOP_REASON_END_TURN, generateMessageResponse.GetStopReason())
	require.InDelta(t, 0.00012, generateMessageResponse.GetGeneratedMessage().GetPrice(), 1e-9)
}

func TestGenerateMessageDiscoveryReturnsToolSchemas(t *testing.T) {
	t.Parallel()
	parent := newChatParent()
	userMessage := newScriptedUserMessage(t,
		newAssistantToolCall(t, "call-1", discoveryToolName, map[string]any{"tools": []any{"ToolB"}}),
		newAssistantText(mock.ToolsPlaceholder),
	)

	// Turn 1: the model discovers ToolB; the server resolves the discovery and
	// returns the tool's schema as the tool call result.
	generateMessageResponse, err := generate(t, parent, newToolSet(), userMessage)
	require.NoError(t, err)
	require.Equal(t, aiservicepb.StopReason_STOP_REASON_TOOL_CALL, generateMessageResponse.GetStopReason())
	toolCall := generateMessageResponse.GetGeneratedMessage().GetBlocks()[0].GetToolCall()
	require.NotNil(t, toolCall)
	toolResult := toolCall.GetResult()
	require.NotNil(t, toolResult)

	discovery := &aipb.ToolCallDiscovery{}
	require.NoError(t, pbutil.UnmarshalFromStruct(discovery, toolResult.GetStructuredContent().GetStructValue()))
	require.Equal(t, toolSetName, discovery.GetToolSetName())
	require.Equal(t, []string{"ToolB"}, discovery.GetToolNames())
	require.Len(t, discovery.GetTools(), 1)
	require.Equal(t, "ToolB", discovery.GetTools()[0].GetName())
	require.NotNil(t, discovery.GetTools()[0].GetJsonSchema())

	// Turn 2: the provider-visible tool list is unchanged by the discovery,
	// preserving the prompt cache.
	generateMessageResponse, err = generate(t, parent, newToolSet(), newToolResultMessage(toolResult))
	require.NoError(t, err)
	require.Equal(t, expectedProviderTools, generateMessageResponse.GetGeneratedMessage().GetBlocks()[0].GetText())
}

func TestGenerateMessageDirectCallToDiscoveredTool(t *testing.T) {
	t.Parallel()
	parent := newChatParent()
	userMessage := newScriptedUserMessage(t,
		newAssistantToolCall(t, "call-1", discoveryToolName, map[string]any{"tools": []any{"ToolB"}}),
		newAssistantToolCall(t, "call-2", "ToolB", map[string]any{"foo": "bar"}),
	)

	// Turn 1: discover ToolB.
	generateMessageResponse, err := generate(t, parent, newToolSet(), userMessage)
	require.NoError(t, err)
	toolResult := generateMessageResponse.GetGeneratedMessage().GetBlocks()[0].GetToolCall().GetResult()
	require.NotNil(t, toolResult)

	// Turn 2: the discovered tool is called directly by name; the call
	// inherits the tool's annotations and is not rejected.
	generateMessageResponse, err = generate(t, parent, newToolSet(), newToolResultMessage(toolResult))
	require.NoError(t, err)
	toolCall := generateMessageResponse.GetGeneratedMessage().GetBlocks()[0].GetToolCall()
	require.NotNil(t, toolCall)
	require.Equal(t, "call-2", toolCall.GetId())
	require.Equal(t, "ToolB", toolCall.GetName())
	require.Equal(t, "bar", toolCall.GetArguments().AsMap()["foo"])
	require.Equal(t, aitool.AnnotationValueToolTypeGenerateMessage, toolCall.GetAnnotations()[aitool.AnnotationKeyToolType])
	require.Equal(t, toolSetName, toolCall.GetAnnotations()[aitool.AnnotationKeyToolSetName])
}
