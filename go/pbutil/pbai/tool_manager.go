package pbai

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	annotationKeyMethod              = "malonaz.pbai.method"
	annotationKeyService             = "malonaz.pbai.service"
	annotationKeyType                = "malonaz.pbai.type"
	annotationValueToolTypeMethod    = "method"
	annotationValueToolTypeDiscovery = "discovery"
)

type ToolManager struct {
	schema   *pbreflection.Schema
	invoker  *pbreflection.MethodInvoker
	maxDepth int
	services map[string]struct{}

	discovery           bool
	serviceToolManagers []*serviceToolManager
	mu                  sync.RWMutex
}

type Option func(*ToolManager)

func WithMaxDepth(depth int) Option {
	return func(m *ToolManager) {
		m.maxDepth = depth
	}
}

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

func NewToolManager(schema *pbreflection.Schema, invoker *pbreflection.MethodInvoker, opts ...Option) *ToolManager {
	m := &ToolManager{
		schema:   schema,
		invoker:  invoker,
		maxDepth: 10,
	}
	for _, opt := range opts {
		opt(m)
	}

	m.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		if m.serviceAllowed(string(svc.FullName())) {
			stm := m.buildServiceToolManager(svc)
			m.serviceToolManagers = append(m.serviceToolManagers, stm)
		}
		return true
	})
	return m
}

func (m *ToolManager) GetTools() []*aipb.Tool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Discovered tools always get appended at the end to leverage prompt caching.
	var discoverTools []*aipb.Tool
	var methodTools []*aipb.Tool
	for _, stm := range m.serviceToolManagers {
		discoverTool, tools := stm.getTools(m.schema)
		if discoverTool != nil {
			discoverTools = append(discoverTools, discoverTool)
		}
		methodTools = append(methodTools, tools...)
	}
	return append(discoverTools, methodTools...)
}

func (m *ToolManager) Execute(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	if toolCall.Metadata == nil {
		return nil, fmt.Errorf("tool call %s missing metadata", toolCall.Name)
	}

	if toolCall.Metadata[annotationKeyType] == annotationValueToolTypeDiscovery {
		return m.executeDiscovery(toolCall)
	}

	return m.executeMethod(ctx, toolCall)
}

type DiscoveryCall struct {
	Service string
	Methods []string
}

func (m *ToolManager) ParseDiscoveryCall(toolCall *aipb.ToolCall) *DiscoveryCall {
	if toolCall.Metadata == nil || toolCall.Metadata[annotationKeyType] != annotationValueToolTypeDiscovery {
		return nil
	}
	dc := &DiscoveryCall{
		Service: toolCall.Metadata[annotationKeyService],
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
	m.mu.Lock()
	defer m.mu.Unlock()

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

type serviceToolManager struct {
	svc                  protoreflect.ServiceDescriptor
	discoverTool         *aipb.Tool
	tools                []*aipb.Tool
	toolNameToDiscovered map[string]bool
}

func (stm *serviceToolManager) getTools(schema *pbreflection.Schema) (*aipb.Tool, []*aipb.Tool) {
	// Separate tools into discovered and undiscovered tools.
	var discoveredTools []*aipb.Tool
	for _, tool := range stm.tools {
		if stm.toolNameToDiscovered[tool.Name] {
			discoveredTools = append(discoveredTools, tool)
		}
	}
	return stm.discoverTool, discoveredTools
}

func (m *ToolManager) buildServiceToolManager(svc protoreflect.ServiceDescriptor) *serviceToolManager {
	stm := &serviceToolManager{
		svc:                  svc,
		toolNameToDiscovered: map[string]bool{},
	}

	// Build method tools.
	methods := svc.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		tool := m.buildMethodTool(method)
		stm.tools = append(stm.tools, tool)
		stm.toolNameToDiscovered[tool.Name] = !m.discovery
	}

	// Always sort to be deterministic and allow for prompt caching.
	sort.Slice(stm.tools, func(i, j int) bool {
		return stm.tools[i].Name < stm.tools[j].Name
	})

	if m.discovery {
		var description strings.Builder
		description.WriteString(fmt.Sprintf("Discover methods of the %s Service.", stm.svc.Name()))
		description.WriteString(fmt.Sprintf("\n%s Service doc: ", stm.svc.Name()))
		description.WriteString(m.schema.GetComment(stm.svc.FullName(), pbreflection.CommentStyleMultiline))
		description.WriteString("\nAvailable methods:")
		var methodNames []string
		for _, tool := range stm.tools {
			methodFQN := tool.Metadata[annotationKeyMethod]
			methodName := methodFQN[strings.LastIndex(methodFQN, ".")+1:]
			methodNames = append(methodNames, methodName)
			description.WriteString("\n- " + methodName)
			methodComment := m.schema.GetComment(protoreflect.FullName(methodFQN), pbreflection.CommentStyleFirstLine)
			if methodComment != "" {
				description.WriteString(": " + methodComment)
			}
		}
		stm.discoverTool = &aipb.Tool{
			Name:        string(stm.svc.Name()) + "_Discover",
			Description: description.String(),
			JsonSchema: &aipb.JsonSchema{
				Type: "object",
				Properties: map[string]*aipb.JsonSchema{
					"methods": {
						Type:        "array",
						Description: "Method names to discover",
						Items:       &aipb.JsonSchema{Type: "string", Enum: methodNames},
					},
				},
				Required: []string{"methods"},
			},
			Metadata: map[string]string{
				annotationKeyType:    annotationValueToolTypeDiscovery,
				annotationKeyService: string(stm.svc.FullName()),
			},
		}
	}

	return stm
}
