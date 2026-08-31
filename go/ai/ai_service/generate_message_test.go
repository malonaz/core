package ai_service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
)

const testToolSetName = "gateway.project.v1.ProjectGateway"

func newDiscoveryToolCall(t *testing.T, toolNames ...string) *aipb.ToolCall {
	t.Helper()
	names := make([]any, 0, len(toolNames))
	for _, toolName := range toolNames {
		names = append(names, toolName)
	}
	arguments, err := structpb.NewStruct(map[string]any{"tools": names})
	require.NoError(t, err)
	return &aipb.ToolCall{
		Id:        "call-1",
		Name:      "ProjectGateway_Discover",
		Arguments: arguments,
		Annotations: map[string]string{
			aitool.AnnotationKeyToolType:    aitool.AnnotationValueToolTypeDiscovery,
			aitool.AnnotationKeyToolSetName: testToolSetName,
		},
	}
}

func newTestToolSetIndex(toolNames ...string) map[string]map[string]*aipb.Tool {
	toolNameToTool := map[string]*aipb.Tool{}
	for _, toolName := range toolNames {
		toolNameToTool[toolName] = &aipb.Tool{Name: toolName, Description: toolName + " description"}
	}
	return map[string]map[string]*aipb.Tool{testToolSetName: toolNameToTool}
}

func discoveredToolsAnnotation(t *testing.T, toolResult *aipb.ToolResult) string {
	t.Helper()
	return toolResult.GetAnnotations()[aitool.AnnotationKeyDiscoveredTools]
}

func TestProcessDiscoveryToolCall(t *testing.T) {
	t.Run("discovers all requested tools", func(t *testing.T) {
		toolSetIndex := newTestToolSetIndex("A", "B", "C")
		toolNameToTool := map[string]*aipb.Tool{}
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "A", "B"), toolSetIndex, toolNameToTool)
		require.Nil(t, toolResult.GetError())
		require.Equal(t, "A,B", discoveredToolsAnnotation(t, toolResult))
		require.Contains(t, toolNameToTool, "A")
		require.Contains(t, toolNameToTool, "B")
	})

	t.Run("already discovered tools are omitted from the result", func(t *testing.T) {
		// Regression: a call naming a mix of new and already-discovered tools
		// returned an error result while still registering the new tools, so the
		// model was told the call failed for tools that were in fact available.
		toolSetIndex := newTestToolSetIndex("A", "B", "C")
		toolNameToTool := map[string]*aipb.Tool{"C": toolSetIndex[testToolSetName]["C"]}
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "A", "B", "C"), toolSetIndex, toolNameToTool)
		require.Nil(t, toolResult.GetError())
		require.Equal(t, "A,B", discoveredToolsAnnotation(t, toolResult))
	})

	t.Run("unknown tools are reported alongside the valid ones", func(t *testing.T) {
		toolSetIndex := newTestToolSetIndex("A", "B")
		toolNameToTool := map[string]*aipb.Tool{}
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "A", "Nope"), toolSetIndex, toolNameToTool)
		require.Nil(t, toolResult.GetError())
		require.Equal(t, "A", discoveredToolsAnnotation(t, toolResult))
		structuredContent := toolResult.GetStructuredContent().GetStructValue().AsMap()
		require.Equal(t, []any{"Nope"}, structuredContent["unknown_tool_names"])
	})

	t.Run("duplicate names within one call are collapsed", func(t *testing.T) {
		toolSetIndex := newTestToolSetIndex("A")
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "A", "A"), toolSetIndex, map[string]*aipb.Tool{})
		require.Nil(t, toolResult.GetError())
		require.Equal(t, "A", discoveredToolsAnnotation(t, toolResult))
	})

	t.Run("errors only when nothing resolves", func(t *testing.T) {
		toolSetIndex := newTestToolSetIndex("A")
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "Nope"), toolSetIndex, map[string]*aipb.Tool{})
		require.NotNil(t, toolResult.GetError())
	})

	t.Run("errors on unknown tool set", func(t *testing.T) {
		toolCall := newDiscoveryToolCall(t, "A")
		toolCall.Annotations[aitool.AnnotationKeyToolSetName] = "other.ToolSet"
		toolResult := processDiscoveryToolCall(toolCall, newTestToolSetIndex("A"), map[string]*aipb.Tool{})
		require.NotNil(t, toolResult.GetError())
	})

	t.Run("nothing new discovered stamps no discovered-tools annotation", func(t *testing.T) {
		// Regression: re-discovering only already-known tools stamped an
		// empty discovered-tools annotation, whose replay ("" is not a tool)
		// poisoned the chat for the rest of its life.
		toolSetIndex := newTestToolSetIndex("A")
		toolNameToTool := map[string]*aipb.Tool{"A": toolSetIndex[testToolSetName]["A"]}
		toolResult := processDiscoveryToolCall(newDiscoveryToolCall(t, "A"), toolSetIndex, toolNameToTool)
		require.Nil(t, toolResult.GetError())
		_, ok := toolResult.GetAnnotations()[aitool.AnnotationKeyDiscoveredTools]
		require.False(t, ok)
	})
}

// fakeStream captures responses forwarded by generateMessageWrapper.Send.
type fakeStream struct {
	pb.AiService_StreamGenerateMessageServer
	responses []*pb.StreamGenerateMessageResponse
}

func (f *fakeStream) Send(response *pb.StreamGenerateMessageResponse) error {
	f.responses = append(f.responses, response)
	return nil
}

func TestGenerateMessageWrapperSend(t *testing.T) {
	t.Run("direct call to a discovered tool succeeds", func(t *testing.T) {
		// Discovered tools are called directly by name: the wrapper copies the
		// tool's annotations onto the call and must not reject it.
		discoveredTool := &aipb.Tool{
			Name: "A",
			Annotations: map[string]string{
				aitool.AnnotationKeyDiscoverableTool: "true",
				aitool.AnnotationKeyToolSetName:      testToolSetName,
			},
		}
		stream := &fakeStream{}
		wrapper := &generateMessageWrapper{
			AiService_StreamGenerateMessageServer: stream,
			messageAccumulator:                    ai.NewMessageAccumulator(),
			toolNameToTool:                        map[string]*aipb.Tool{"A": discoveredTool},
			toolCallIDToToolCall:                  map[string]*aipb.ToolCall{},
		}
		toolCall := &aipb.ToolCall{Id: "call-1", Name: "A"}
		response := &pb.StreamGenerateMessageResponse{
			Content: &pb.StreamGenerateMessageResponse_Block{Block: ai.NewToolCallBlock(toolCall)},
		}
		require.NoError(t, wrapper.Send(response))
		require.Len(t, stream.responses, 1)
		sentToolCall := stream.responses[0].GetBlock().GetToolCall()
		require.Equal(t, "A", sentToolCall.GetName())
		require.Equal(t, "true", sentToolCall.GetAnnotations()[aitool.AnnotationKeyDiscoverableTool])
	})

	t.Run("unknown tool returns a recoverable error", func(t *testing.T) {
		wrapper := &generateMessageWrapper{
			AiService_StreamGenerateMessageServer: &fakeStream{},
			messageAccumulator:                    ai.NewMessageAccumulator(),
			toolNameToTool:                        map[string]*aipb.Tool{},
			toolCallIDToToolCall:                  map[string]*aipb.ToolCall{},
		}
		toolCall := &aipb.ToolCall{Id: "call-1", Name: "Nope"}
		response := &pb.StreamGenerateMessageResponse{
			Content: &pb.StreamGenerateMessageResponse_Block{Block: ai.NewToolCallBlock(toolCall)},
		}
		require.Error(t, wrapper.Send(response))
	})
}
