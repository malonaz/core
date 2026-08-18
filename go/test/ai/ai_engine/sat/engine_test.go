package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
)

const serviceFullName = "malonaz.ai.ai_engine.v1.AiEngine"

func createServiceToolSet(t *testing.T, discoveredMethodNames ...string) *aipb.ToolSet {
	t.Helper()
	createServiceToolSetRequest := &aienginepb.CreateServiceToolSetRequest{
		ServiceFullName:       serviceFullName,
		DiscoveredMethodNames: discoveredMethodNames,
	}
	toolSet, err := aiEngineClient.CreateServiceToolSet(ctx, createServiceToolSetRequest)
	require.NoError(t, err)
	// The self-reflection descriptors carry no source comments, so tool
	// descriptions come back empty; backfill them to satisfy the
	// ParseToolCallRequest validation when the tool set is echoed back.
	for _, tool := range toolSet.GetTools() {
		if tool.GetDescription() == "" {
			tool.Description = tool.GetName()
		}
	}
	return toolSet
}

func toolByName(t *testing.T, toolSet *aipb.ToolSet, name string) *aipb.Tool {
	t.Helper()
	for _, tool := range toolSet.GetTools() {
		if tool.GetName() == name {
			return tool
		}
	}
	t.Fatalf("tool %q not found in tool set %q", name, toolSet.GetName())
	return nil
}

func TestCreateServiceToolSet(t *testing.T) {
	toolSet := createServiceToolSet(t, "CreateTool")
	require.Equal(t, serviceFullName, toolSet.GetName())
	require.Equal(t, "AiEngine_Discover", toolSet.GetDiscoveryTool().GetName())
	require.Equal(t, serviceFullName, toolSet.GetDiscoveryTool().GetAnnotations()[aitool.AnnotationKeyToolSetName])

	// CreateTool is pre-discovered: available without a discovery tool call.
	preDiscoveredTool := toolByName(t, toolSet, "AiEngine_CreateTool")
	require.Equal(t, aip.LabelValueTrue, preDiscoveredTool.GetAnnotations()[aitool.AnnotationKeyPreDiscoveredTool])
	require.Equal(t, aip.LabelValueFalse, preDiscoveredTool.GetAnnotations()[aitool.AnnotationKeyDiscoverableTool])

	// Every other method is discoverable, and called directly once discovered.
	discoverableTool := toolByName(t, toolSet, "AiEngine_ParseToolCall")
	require.Equal(t, aip.LabelValueTrue, discoverableTool.GetAnnotations()[aitool.AnnotationKeyDiscoverableTool])
	require.Equal(t, aitool.AnnotationValueToolTypeGenerateRPCRequest, discoverableTool.GetAnnotations()[aitool.AnnotationKeyToolType])
	require.NotNil(t, discoverableTool.GetJsonSchema())
}

func TestParseToolCallDiscovery(t *testing.T) {
	toolSet := createServiceToolSet(t)
	arguments, err := structpb.NewStruct(map[string]any{"tools": []any{"AiEngine_CreateTool"}})
	require.NoError(t, err)
	parseToolCallRequest := &aienginepb.ParseToolCallRequest{
		ToolCall: &aipb.ToolCall{
			Id:          "call-1",
			Name:        toolSet.GetDiscoveryTool().GetName(),
			Arguments:   arguments,
			Annotations: toolSet.GetDiscoveryTool().GetAnnotations(),
		},
		ToolSets: []*aipb.ToolSet{toolSet},
	}
	parseToolCallResponse, err := aiEngineClient.ParseToolCall(ctx, parseToolCallRequest)
	require.NoError(t, err)
	discovery := parseToolCallResponse.GetDiscovery()
	require.NotNil(t, discovery)
	require.Equal(t, serviceFullName, discovery.GetToolSetName())
	require.Equal(t, []string{"AiEngine_CreateTool"}, discovery.GetToolNames())
}

func TestParseToolCallDirect(t *testing.T) {
	// Discovered tools are called directly by name: parsing resolves the
	// tool's RPC from its own annotations.
	toolSet := createServiceToolSet(t)
	tool := toolByName(t, toolSet, "AiEngine_CreateDiscoveryTool")
	arguments, err := structpb.NewStruct(map[string]any{"name": "MyDiscover", "description": "My discovery tool"})
	require.NoError(t, err)
	parseToolCallRequest := &aienginepb.ParseToolCallRequest{
		ToolCall: &aipb.ToolCall{
			Id:          "call-1",
			Name:        tool.GetName(),
			Arguments:   arguments,
			Annotations: tool.GetAnnotations(),
		},
		ToolSets: []*aipb.ToolSet{toolSet},
	}
	parseToolCallResponse, err := aiEngineClient.ParseToolCall(ctx, parseToolCallRequest)
	require.NoError(t, err)

	rpc := parseToolCallResponse.GetRpc()
	require.NotNil(t, rpc)
	require.Equal(t, serviceFullName, rpc.GetServiceFullName())
	require.Equal(t, serviceFullName+".CreateDiscoveryTool", rpc.GetMethodFullName())
	require.Equal(t, "MyDiscover", rpc.GetRequest().AsMap()["name"])
}
