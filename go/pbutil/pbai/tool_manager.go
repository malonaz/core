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
	annotationKeyMethod  = "malonaz.pbai.method"
	annotationKeyService = "malonaz.pbai.service"
	annotationKeyType    = "malonaz.pbai.type"
	toolTypeDiscovery    = "discovery"
)

type ToolProvider interface {
	GetTools() []*aipb.Tool
}

type ToolManager struct {
	schema   *pbreflection.Schema
	invoker  *pbreflection.MethodInvoker
	maxDepth int
	services map[string]struct{}

	discovery bool
	enabled   map[string]struct{}
	mu        sync.RWMutex
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
		m.enabled = make(map[string]struct{})
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
	return m
}

func (m *ToolManager) GetTools() []*aipb.Tool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var tools []*aipb.Tool
	m.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		if !m.serviceAllowed(string(svc.FullName())) {
			return true
		}
		st := m.buildServiceTools(svc)
		tools = append(tools, st.getTools(m.discovery, m.enabled)...)
		return true
	})

	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})
	return tools
}

func (m *ToolManager) Execute(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	if toolCall.Metadata == nil {
		return nil, fmt.Errorf("tool call %s missing metadata", toolCall.Name)
	}

	if toolCall.Metadata[annotationKeyType] == toolTypeDiscovery {
		return m.executeDiscovery(toolCall)
	}

	return m.executeMethod(ctx, toolCall)
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

	desc, err := m.schema.Files.FindDescriptorByName(protoreflect.FullName(serviceFQN))
	if err != nil {
		return fmt.Errorf("service not found: %s", serviceFQN)
	}
	svc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return fmt.Errorf("not a service: %s", serviceFQN)
	}

	methods := svc.Methods()
	for _, name := range methodNames {
		method := methods.ByName(protoreflect.Name(name))
		if method == nil {
			return fmt.Errorf("method not found: %s.%s", serviceFQN, name)
		}
		m.enabled[string(method.FullName())] = struct{}{}
	}
	return nil
}

type serviceTools struct {
	svc           protoreflect.ServiceDescriptor
	discoveryTool *aipb.Tool
	methodTools   []*aipb.Tool
}

func (st *serviceTools) getTools(discovery bool, enabled map[string]struct{}) []*aipb.Tool {
	if !discovery {
		return st.methodTools
	}

	var tools []*aipb.Tool
	hasUnenabled := false
	for _, mt := range st.methodTools {
		methodFQN := mt.Metadata[annotationKeyMethod]
		if _, ok := enabled[methodFQN]; ok {
			tools = append(tools, mt)
		} else {
			hasUnenabled = true
		}
	}
	if hasUnenabled {
		tools = append(tools, st.discoveryTool)
	}
	return tools
}

func (m *ToolManager) buildServiceTools(svc protoreflect.ServiceDescriptor) *serviceTools {
	st := &serviceTools{svc: svc}

	methods := svc.Methods()
	var methodLines []string
	var methodNames []string

	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		st.methodTools = append(st.methodTools, m.buildMethodTool(method))

		name := string(method.Name())
		methodNames = append(methodNames, name)
		doc := m.schema.Comments[string(method.FullName())]
		if doc != "" {
			firstLine := strings.Split(doc, "\n")[0]
			methodLines = append(methodLines, fmt.Sprintf("- %s: %s", name, firstLine))
		} else {
			methodLines = append(methodLines, fmt.Sprintf("- %s", name))
		}
	}

	svcDoc := m.schema.Comments[string(svc.FullName())]
	description := svcDoc
	if description != "" {
		description += "\n\n"
	}
	description += "Methods:\n" + strings.Join(methodLines, "\n")

	st.discoveryTool = &aipb.Tool{
		Name:        string(svc.Name()) + "_Discover",
		Description: description,
		JsonSchema: &aipb.JsonSchema{
			Type: "object",
			Properties: map[string]*aipb.JsonSchema{
				"methods": {
					Type:        "array",
					Description: "Method names to enable",
					Items:       &aipb.JsonSchema{Type: "string", Enum: methodNames},
				},
			},
			Required: []string{"methods"},
		},
		Metadata: map[string]string{
			annotationKeyService: string(svc.FullName()),
			annotationKeyType:    toolTypeDiscovery,
		},
	}

	return st
}
