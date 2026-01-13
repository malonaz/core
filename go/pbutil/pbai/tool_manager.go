package pbai

import (
	"fmt"

	"github.com/malonaz/core/go/ai"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	defaultMaxDepth = 10

	annotationKeyPrefix      = "pbai.malonaz.com/"
	annotationKeyGRPCService = annotationKeyPrefix + "grpc-service"
	annotationKeyGRPCMethod  = annotationKeyPrefix + "grpc-method"
)

type toolManagerConfig struct {
	maxDepth int
	services map[string]struct{}
}

type Option func(*toolManagerConfig)

func WithServices(services ...string) Option {
	return func(c *toolManagerConfig) {
		for _, s := range services {
			c.services[s] = struct{}{}
		}
	}
}

func WithMaxDepth(maxDepth int) Option {
	return func(c *toolManagerConfig) {
		c.maxDepth = maxDepth
	}
}

type ToolManager struct {
	config               *toolManagerConfig
	schema               *pbreflection.Schema
	schemaBuilder        *pbjson.SchemaBuilder
	discoverableToolSets []*ai.DiscoverableToolSet
}

func NewToolManager(schema *pbreflection.Schema, opts ...Option) (*ToolManager, error) {
	config := &toolManagerConfig{
		maxDepth: defaultMaxDepth,
		services: map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(config)
	}

	m := &ToolManager{
		schema:        schema,
		config:        config,
		schemaBuilder: pbjson.NewSchemaBuilder(schema),
	}

	var err error
	m.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		if m.serviceAllowed(string(svc.FullName())) {
			var tools []*aipb.Tool
			tools, err = m.buildServiceTools(svc)
			if err != nil {
				return false
			}
			discoverableToolSet := ai.NewDiscoverableToolSet(string(svc.Name()), tools)
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

func (m *ToolManager) ProcessDiscoveryToolCall(toolCall *aipb.ToolCall) (*ai.DiscoveryCall, error) {
	discoverableToolSet, err := m.getDiscoverableToolSet(toolCall)
	if err != nil {
		return nil, err
	}
	if !discoverableToolSet.IsDiscoveryTool(toolCall.Name) {
		return nil, status.Errorf(codes.FailedPrecondition, "tool call %s is not a discovery tool call", toolCall.Name)
	}
	return discoverableToolSet.ProcessDiscoveryToolCall(toolCall)
}

type MethodToolCall struct {
	Method  protoreflect.MethodDescriptor
	Request *dynamicpb.Message
}

func (m *ToolManager) ParseMethodToolCall(toolCall *aipb.ToolCall) (*MethodToolCall, error) {
	// Ensure that the tool name is valid.
	discoverableToolSet, err := m.getDiscoverableToolSet(toolCall)
	if err != nil {
		return nil, err
	}
	if !discoverableToolSet.IsDiscovered(toolCall.Name) {
		return nil, status.Errorf(codes.FailedPrecondition, "must discover tool call %s before using it", toolCall.Name)
	}
	if toolCall.Annotations == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "tool call %s missing annotations", toolCall.Name)
	}
	methodFQN := toolCall.Annotations[annotationKeyGRPCMethod]
	if methodFQN == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "tool call %s missing method annotation", toolCall.Name)
	}
	desc, err := m.schema.FindDescriptorByName(protoreflect.FullName(methodFQN))
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "tool call %s refers to unknown descriptor: %s", toolCall.Name, methodFQN)
	}
	method, ok := desc.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "tool call %s refers to unknown method: %s", toolCall.Name, methodFQN)
	}
	message, err := pbjson.BuildMessage(method.Input(), toolCall.Arguments.AsMap())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building message %s: %v", method.Input(), err)
	}

	return &MethodToolCall{
		Method:  method,
		Request: message,
	}, nil
}

func (m *ToolManager) serviceAllowed(name string) bool {
	if len(m.config.services) == 0 {
		return true
	}
	_, ok := m.config.services[name]
	return ok
}

func (m *ToolManager) buildServiceTools(svc protoreflect.ServiceDescriptor) ([]*aipb.Tool, error) {
	var tools []*aipb.Tool

	// Build method tools.
	methods := svc.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		tool, err := m.buildMethodTool(svc, method)
		if err != nil {
			return nil, fmt.Errorf("building tool for method %s", method.FullName())
		}
		tools = append(tools, tool)
	}

	return tools, nil
}

func (m *ToolManager) getDiscoverableToolSet(toolCall *aipb.ToolCall) (*ai.DiscoverableToolSet, error) {
	for _, discoverableToolSet := range m.discoverableToolSets {
		if discoverableToolSet.HasTool(toolCall.Name) {
			return discoverableToolSet, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "unknown tool: %s", toolCall.Name)
}

func toolName(serviceName, methodName protoreflect.Name) string {
	return string(string(serviceName) + "_" + string(methodName))
}

func (m *ToolManager) buildMethodTool(service protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor) (*aipb.Tool, error) {
	description := m.schema.GetComment(method.FullName(), pbreflection.CommentStyleMultiline)

	standardMethodType := m.schema.GetStandardMethodType(method.FullName())
	schema, err := m.schemaBuilder.BuildSchema(method.Input().FullName(), standardMethodType, pbjson.WithMaxDepth(m.config.maxDepth))
	if err != nil {
		return nil, fmt.Errorf("building schema for: %v", method.Input().FullName())
	}

	return &aipb.Tool{
		Name:        toolName(service.Name(), method.Name()),
		Description: description,
		JsonSchema:  schema,
		Annotations: map[string]string{
			annotationKeyGRPCService: string(service.FullName()),
			annotationKeyGRPCMethod:  string(method.FullName()),
		},
	}, nil
}
