package ai

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

const (
	annotationKeyPrefix              = "ai.malonaz.com/"
	annotationKeyToolType            = annotationKeyPrefix + "tool-type"
	annotationValueToolTypeDiscovery = "discovery"
	annotationKeyGRPCService         = annotationKeyPrefix + "grpc-service"
	annotationKeyGRPCMethod          = annotationKeyPrefix + "grpc-method"
)

type DiscoverableToolSet struct {
	name                   string
	discoveryTool          *aipb.Tool
	tools                  []*aipb.Tool
	toolNameToDiscoverTime map[string]time.Time
}

func NewDiscoverableToolSet(name string, tools []*aipb.Tool) *DiscoverableToolSet {
	ts := &DiscoverableToolSet{
		name:                   name,
		tools:                  tools,
		toolNameToDiscoverTime: make(map[string]time.Time, len(tools)),
	}

	var toolNames []string
	for _, tool := range tools {
		ts.toolNameToDiscoverTime[tool.Name] = time.Time{}
		toolNames = append(toolNames, tool.Name)
	}

	var desc strings.Builder
	desc.WriteString(fmt.Sprintf("Discover tools from %s.", name))
	desc.WriteString("\nAvailable tools:")
	for _, tool := range tools {
		desc.WriteString("\n- " + tool.Name)
		if tool.Description != "" {
			firstLine := strings.SplitN(tool.Description, "\n", 2)[0]
			desc.WriteString(": " + firstLine)
		}
	}

	ts.discoveryTool = &aipb.Tool{
		Name:        name + "_Discover",
		Description: desc.String(),
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
			annotationKeyToolType: annotationValueToolTypeDiscovery,
		},
	}

	ts.sortTools()
	return ts
}

func IsDiscoveryToolCall(toolCall *aipb.ToolCall) bool {
	return toolCall.Annotations != nil && toolCall.Annotations[annotationKeyToolType] == annotationValueToolTypeDiscovery
}

func (ts *DiscoverableToolSet) sortTools() {
	sort.Slice(ts.tools, func(i, j int) bool {
		ti := ts.toolNameToDiscoverTime[ts.tools[i].Name]
		tj := ts.toolNameToDiscoverTime[ts.tools[j].Name]
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return ts.tools[i].Name < ts.tools[j].Name
	})
}

func (ts *DiscoverableToolSet) IsDiscovered(toolName string) bool {
	return !ts.toolNameToDiscoverTime[toolName].IsZero()
}

func (ts *DiscoverableToolSet) HasTool(toolName string) bool {
	_, ok := ts.toolNameToDiscoverTime[toolName]
	return ok || ts.IsDiscoveryTool(toolName)
}

func (ts *DiscoverableToolSet) IsDiscoveryTool(toolName string) bool {
	return toolName == ts.discoveryTool.Name
}

type DiscoveryCall struct {
	ToolSetName string
	ToolNames   []string
}

func (ts *DiscoverableToolSet) ProcessDiscoveryToolCall(toolCall *aipb.ToolCall) (*DiscoveryCall, error) {
	if !ts.IsDiscoveryTool(toolCall.Name) {
		return nil, status.Errorf(codes.Internal, "Cannot process non-discovery tool call")
	}

	// Parse discovery call.
	args := toolCall.Arguments.AsMap()
	toolsRaw, ok := args["tools"]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing tools argument")
	}
	toolsArr, ok := toolsRaw.([]any)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "tools must be an array")
	}

	discoveryCall := &DiscoveryCall{
		ToolSetName: ts.name,
	}
	for _, v := range toolsArr {
		toolName, ok := v.(string)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "tool name must be a string")
		}
		discoverTime, ok := ts.toolNameToDiscoverTime[toolName]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "unknown tool %s", toolName)
		}
		if discoverTime.IsZero() {
			// We allow re-discovery of tools as it's less expensive than to return an error informing
			// the LLM that they've already discovered this tool.
			ts.toolNameToDiscoverTime[toolName] = time.Now()
		}
		discoveryCall.ToolNames = append(discoveryCall.ToolNames, toolName)
	}

	ts.sortTools()
	return discoveryCall, nil
}

func (ts *DiscoverableToolSet) GetTools() (*aipb.Tool, []*aipb.Tool) {
	var tools []*aipb.Tool
	for _, tool := range ts.tools {
		if !ts.toolNameToDiscoverTime[tool.Name].IsZero() {
			tools = append(tools, tool)
		}
	}
	var discoveryTool *aipb.Tool
	if len(tools) < len(ts.tools) {
		discoveryTool = ts.discoveryTool
	}
	return discoveryTool, tools
}
