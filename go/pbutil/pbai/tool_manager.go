package pbai

import (
	"context"
	"fmt"

	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	annotationKeyPrefix              = "ai.malonaz.com/"
	annotationKeyGRPCService         = annotationKeyPrefix + "grpc-service"
	annotationKeyGRPCMethod          = annotationKeyPrefix + "grpc-method"
	annotationKeyToolType            = annotationKeyPrefix + "tool-type"
	annotationValueToolTypeMethod    = "method"
	annotationValueToolTypeDiscovery = "discovery"
)

type ToolManager struct {
	schema        *pbreflection.Schema
	invoker       *pbreflection.MethodInvoker
	schemaBuilder *pbjson.SchemaBuilder
	services      map[string]struct{}

	discovery           bool
	discoverableToolSets []*ai.DiscoverableToolSet
}

type Option func(*ToolManager)

func WithServices(services ...string) Option {
	return func(m *ToolManager) {
		m.services = make(map[string]struct{})
		for _, s := range services {
			m.services[s] = struct{}{}
		}
	}
}

func WithDiscovery() Option {
	return func(m *ToolManager) {
		m.discovery = true
	}
}

func NewToolManager(schema *pbreflection.Schema, invoker *pbreflection.MethodInvoker, opts ...Option) (*ToolManager, error) {
	m := &ToolManager{
		schema:  schema,
		invoker: invoker,
	}
	for _, opt := range opts {
		opt(m)
	}
	m.schemaBuilder = pbjson.NewSchemaBuilder(schema)

	var err error
	m.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		if m.serviceAllowed(string(svc.FullName())) {
			var tools []*aipb.Tool
			tools, err = m.buildServiceTools(svc)
			if err != nil {
				return false
			}
			discoverableToolSet := ai.NewDiscoverableToolSet(string(svc.Name()) + " Service", tools)
			m.discoverableToolSets = append(m.discoverableToolSets, discoverableToolSet)
		}
		return true
	})

	return m, err
}

func (m *ToolManager) GetTools() []*aipb.Tool {
	// Discovered tools always get appended at the end to leverage prompt caching.
	var discoverTools []*aipb.Tool
	var methodTools []*aipb.Tool
	for _, discoverableToolSet := range m.discoverableToolSets {
		discoverTool, tools := discoverableToolSet.GetTools()
		if discoverTool != nil {
			discoverTools = append(discoverTools, discoverTool)
		}
		methodTools = append(methodTools, tools...)
	}
	return append(discoverTools, methodTools...)
}

func (m *ToolManager) Execute(ctx context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	message, err := m.ExecuteRaw(ctx, toolCall)
	if err != nil {
		return ai.NewErrorToolResult(err), nil
	}
	jsonBytes, err := pbutil.JSONMarshal(message)
	if err != nil {
		return nil, err
	}
	value := &structpb.Value{}
	if err := pbutil.JSONUnmarshal(jsonBytes, value); err != nil {
		return nil, err
	}
	return ai.NewStructuredToolResult(value), nil
}

func (m *ToolManager) ExecuteRaw(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	// Ensure that the tool name is valid.
	var discoverableToolSet *ai.DiscoverableToolSet
	for _, discoverableToolSet = range m.discoverableToolSets {
		if discoverableToolSet.HasTool(toolCall.Name) {
			break
		}
	}
	// Throw a not found error if the tool is not found.
	if discoverableToolSet == nil {
		return nil, status.Errorf(codes.NotFound, "unknown tool: %s", toolCall.Name)
	}

	if discoverableToolSet.DiscoveryToolName()
	if toolCall.Annotations == nil {
		return nil, fmt.Errorf("tool call %s missing annotations", toolCall.Name)
	}

	if toolCall.Annotations[annotationKeyToolType] == annotationValueToolTypeDiscovery {
		return m.executeDiscovery(toolCall)
	}

	return m.executeMethod(ctx, toolCall)
}

type DiscoveryCall struct {
	Service string
	Methods []string
}

func (m *ToolManager) ParseDiscoveryCall(toolCall *aipb.ToolCall) *DiscoveryCall {
	if toolCall.Annotations == nil || toolCall.Annotations[annotationKeyToolType] != annotationValueToolTypeDiscovery {
		return nil
	}
	dc := &DiscoveryCall{
		Service: toolCall.Annotations[annotationKeyGRPCService],
	}
	if args := toolCall.Arguments.AsMap(); args != nil {
		if methodsRaw, ok := args["methods"].([]any); ok {
			for _, v := range methodsRaw {
				if s, ok := v.(string); ok {
					dc.Methods = append(dc.Methods, s)
				}
			}
		}
	}
	return dc
}

func (m *ToolManager) serviceAllowed(name string) bool {
	if m.services == nil {
		return true
	}
	_, ok := m.services[name]
	return ok
}

func (m *ToolManager) enableMethods(serviceFQN string, methodNames []string) error {
	for _, stm := range m.serviceToolManagers {
		if string(stm.svc.FullName()) != serviceFQN {
			continue
		}
		for _, methodName := range methodNames {
			toolName := toolName(stm.svc.Name(), protoreflect.Name(methodName))
			if _, ok := stm.toolNameToDiscovered[toolName]; !ok {
				return fmt.Errorf("unknown tool %s", toolName)
			}
			stm.toolNameToDiscovered[toolName] = true
		}
		break
	}
	return nil
}

func (m *ToolManager) buildServiceTools(svc protoreflect.ServiceDescriptor) ([]*aipb.Tool, error) {
	var tools []*aipb.Tool

	// Build method tools.
	methods := svc.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		tool, err := m.buildMethodTool(method)
		if err != nil {
			return nil, fmt.Errorf("building tool for method %s", method.FullName())
		}
		tools = append(tools, tool)
	}

	return tools, nil
}
