package tool

import (
	"strings"

	"github.com/malonaz/core/go/aip"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

type CreateDiscoveryToolRequest struct {
	Name        string
	Description string
	Tools       []*aipb.Tool
}

func CreateDiscoveryTool(request *CreateDiscoveryToolRequest) *aipb.Tool {
	toolNames := make([]string, 0, len(request.Tools))
	for _, tool := range request.Tools {
		toolNames = append(toolNames, tool.Name)
	}

	var description strings.Builder
	if request.Description != "" {
		description.WriteString(request.Description)
		description.WriteString("\n\n")
	}
	description.WriteString("After discovering tools, call them directly by name using their discovered schemas. Discover the following tools:")
	for _, tool := range request.Tools {
		description.WriteString("\n- " + tool.Name)
		if tool.Description != "" {
			firstLine := strings.SplitN(tool.Description, "\n", 2)[0]
			description.WriteString(": " + firstLine)
		}
	}

	return &aipb.Tool{
		Name:        request.Name,
		Description: description.String(),
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"tools": {
					Type:        "array",
					Description: "Tool names to discover",
					Items:       &jsonpb.Schema{Type: "string", Enum: toolNames},
				},
			},
			Required: []string{"tools"},
		},
		Annotations: map[string]string{
			aipb.Annotations.ToolType.Key:     ToolTypeDiscovery,
			aipb.Annotations.NoSideEffect.Key: "true",
		},
	}
}

// NewToolSet returns the set a model discovers tools through: each tool annotated as discoverable in it, and its
// discovery tool listing them.
func NewToolSet(name, discoveryToolName, description string, tools []*aipb.Tool) *aipb.ToolSet {
	for _, tool := range tools {
		aipb.Annotations.ToolSetName.Set(tool, name)
		aipb.Annotations.DiscoverableTool.Set(tool, aip.LabelValueTrue)
	}
	discoveryTool := CreateDiscoveryTool(&CreateDiscoveryToolRequest{Name: discoveryToolName, Description: description, Tools: tools})
	aipb.Annotations.ToolSetName.Set(discoveryTool, name)
	return &aipb.ToolSet{Name: name, DiscoveryTool: discoveryTool, Tools: tools}
}
