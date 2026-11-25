package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// mirroredSet is the tool-set label value of the ai service, which the sat's processor mirrors.
const mirroredSet = "malonaz-ai-ai_service-v1-aiservice"

// awaitTool returns a platform tool once a mirroring pass wrote it.
func awaitTool(t *testing.T, name string) *aipb.Tool {
	t.Helper()
	var tool *aipb.Tool
	require.Eventually(t, func() bool {
		var err error
		tool, err = aiServiceClient.GetTool(ctx, &aiservicepb.GetToolRequest{Name: name})
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)
	return tool
}

// A target's unary methods are mirrored into platform tools carrying what their descriptors declare.
func TestProcessor_MirrorsATargetsMethods(t *testing.T) {
	t.Parallel()
	tool := awaitTool(t, "tools/ai-service-get-decision")
	// Models call it by the name they always did.
	require.Equal(t, "AiService_GetDecision", tool.GetTitle())
	require.Equal(t, mirroredSet, tool.GetLabels()[aipb.Labels.ToolSet.GetKey()])
	toolMethod, ok, err := aipb.Annotations.Method.Get(tool)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService.GetDecision", toolMethod.GetMethod())
	require.True(t, toolMethod.GetReadOnly())
	require.NotNil(t, tool.GetJsonSchema())

	// Streaming methods are no tools.
	_, err = aiServiceClient.GetTool(ctx, &aiservicepb.GetToolRequest{Name: "tools/ai-service-stream-generate-message"})
	require.True(t, status.HasCode(err, codes.NotFound))
}

// A pass deletes the set's tools its target no longer serves, and leaves unchanged tools untouched.
func TestProcessor_Converges(t *testing.T) {
	t.Parallel()
	tool := awaitTool(t, "tools/ai-service-get-decision")

	stale := &aipb.Tool{
		Title:       "AiService_Retired",
		Description: "A method the ai service no longer serves.",
		JsonSchema:  tool.GetJsonSchema(),
		Labels:      map[string]string{aipb.Labels.ToolSet.GetKey(): mirroredSet},
	}
	createToolRequest := &aiservicepb.CreateToolRequest{Tool: stale, ToolId: "ai-service-retired"}
	_, err := aiServiceClient.CreateTool(ctx, createToolRequest)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := aiServiceClient.GetTool(ctx, &aiservicepb.GetToolRequest{Name: "tools/ai-service-retired"})
		return status.HasCode(err, codes.NotFound)
	}, 10*time.Second, 100*time.Millisecond)

	unchanged, err := aiServiceClient.GetTool(ctx, &aiservicepb.GetToolRequest{Name: tool.GetName()})
	require.NoError(t, err)
	require.Equal(t, tool.GetEtag(), unchanged.GetEtag())
	require.Equal(t, tool.GetUpdateTime().AsTime(), unchanged.GetUpdateTime().AsTime())
}
