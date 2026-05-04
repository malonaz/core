package tool

import (
	"strings"

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
	description.WriteString("Discover the following tools:")
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
			AnnotationKeyToolType:     AnnotationValueToolTypeDiscovery,
			AnnotationKeyNoSideEffect: "true",
		},
	}
}
