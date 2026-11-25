package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// newSetTool returns a tool in the set named setID.
func newSetTool(setID string) *aipb.Tool {
	return &aipb.Tool{
		Title:       "Quotes_List",
		Description: "Lists the quotes.",
		JsonSchema:  &jsonpb.Schema{Type: "object"},
		Labels:      map[string]string{aipb.Labels.ToolSet.GetKey(): setID},
	}
}

// A platform tool and an organization's tool live under their own parents; a set is listed by its label.
func TestTool_PlatformAndOrganizationTools(t *testing.T) {
	t.Parallel()
	setID := "set-" + uuid.MustNewV7().String()[:8]
	toolID := "tools-" + uuid.MustNewV7().String()[:8]
	organization := "organizations/" + uuid.MustNewV7().String()

	createToolRequest := &aiservicepb.CreateToolRequest{Tool: newSetTool(setID), ToolId: toolID}
	platformTool, err := aiServiceClient.CreateTool(ctx, createToolRequest)
	require.NoError(t, err)
	require.Equal(t, "tools/"+toolID, platformTool.GetName())

	createToolRequest = &aiservicepb.CreateToolRequest{Parent: organization, Tool: newSetTool(setID), ToolId: toolID}
	organizationTool, err := aiServiceClient.CreateTool(ctx, createToolRequest)
	require.NoError(t, err)
	require.Equal(t, organization+"/tools/"+toolID, organizationTool.GetName())

	filter := `labels."ai.malonaz.com/tool-set" = "` + setID + `"`
	listToolsRequest := &aiservicepb.ListToolsRequest{Filter: filter}
	listToolsResponse, err := aiServiceClient.ListTools(ctx, listToolsRequest)
	require.NoError(t, err)
	require.Len(t, listToolsResponse.GetTools(), 1)
	require.Equal(t, platformTool.GetName(), listToolsResponse.GetTools()[0].GetName())

	listToolsRequest = &aiservicepb.ListToolsRequest{Parent: organization, Filter: filter}
	listToolsResponse, err = aiServiceClient.ListTools(ctx, listToolsRequest)
	require.NoError(t, err)
	require.Len(t, listToolsResponse.GetTools(), 1)
	require.Equal(t, organizationTool.GetName(), listToolsResponse.GetTools()[0].GetName())
}

// A stored tool's id follows the house convention; the name a model calls it by is its title, which it must have.
func TestTool_TitleIsTheModelFacingName(t *testing.T) {
	t.Parallel()
	createToolRequest := &aiservicepb.CreateToolRequest{Tool: newSetTool("set"), ToolId: "Quotes_List"}
	_, err := aiServiceClient.CreateTool(ctx, createToolRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)

	untitled := newSetTool("set")
	untitled.Title = ""
	createToolRequest = &aiservicepb.CreateToolRequest{Tool: untitled}
	_, err = aiServiceClient.CreateTool(ctx, createToolRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}
