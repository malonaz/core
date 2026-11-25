package ai_processor

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aienginepb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbreflection"
	"github.com/malonaz/core/go/uuid"
)

// toolRequestIDNamespace derives the request id of a mirrored tool's create from its name, so a pass finds the tool
// another pass created.
var toolRequestIDNamespace = uuid.MustParse("0f3c9a6e-5b27-4d84-a1e9-7c2d84b5f013")

var targetErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ai_processor_target_errors_total",
	Help: "Mirroring passes that could not read a target or write its tools.",
}, []string{"target"})

var toolSetIDPattern = regexp.MustCompile(`^[a-z0-9_-]{1,63}$`)

// toolSetID returns the tool-set label value of a service: its full name lowercased, dots as dashes.
func toolSetID(service string) (string, error) {
	id := strings.ReplaceAll(strings.ToLower(service), ".", "-")
	if !toolSetIDPattern.MatchString(id) {
		return "", fmt.Errorf("service %s makes no tool-set label value: %q exceeds 63 characters of [a-z0-9_-]", service, id)
	}
	return id, nil
}

// toolID returns the id of a method's platform tool: its service's and its own names in kebab case
// (ProjectGateway.ListQuotes is project-gateway-list-quotes).
func toolID(method protoreflect.MethodDescriptor) string {
	return kebabCase(string(method.Parent().Name())) + "-" + kebabCase(string(method.Name()))
}

// kebabCase lowercases a CamelCase name, a dash before each word: AIService is ai-service.
func kebabCase(name string) string {
	var builder strings.Builder
	runes := []rune(name)
	for i, r := range runes {
		if r == '_' {
			r = '-'
		}
		startsWord := i > 0 && unicode.IsUpper(r) &&
			(!unicode.IsUpper(runes[i-1]) || i+1 < len(runes) && unicode.IsLower(runes[i+1]))
		if startsWord && runes[i-1] != '_' {
			builder.WriteRune('-')
		}
		builder.WriteRune(unicode.ToLower(r))
	}
	return builder.String()
}

// mirror mirrors every target, each on its own: one failing leaves the others mirrored.
func (s *Service) mirror(ctx context.Context) error {
	// Two services may share a short name: a tool id taken by an earlier target fails the later one.
	nameToService := map[string]string{}
	for _, target := range s.targets {
		if err := s.mirrorTarget(ctx, target, nameToService); err != nil {
			targetErrors.WithLabelValues(target.service).Inc()
			s.log.WarnContext(ctx, "mirroring a tool target", "target", target.service, "endpoint", target.endpoint, "error", err)
		}
	}
	return nil
}

// mirrorTarget makes the platform tools of a target's set its unary methods' tools, as its reflection reads now.
func (s *Service) mirrorTarget(ctx context.Context, target target, nameToService map[string]string) error {
	connection, err := s.connection(ctx, target.endpoint)
	if err != nil {
		return err
	}
	reflectionClient := grpc_reflection_v1.NewServerReflectionClient(connection.Get())
	schema, err := pbreflection.ResolveSchema(ctx, reflectionClient)
	if err != nil {
		return fmt.Errorf("reflecting %s: %w", target.endpoint, err)
	}
	descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(target.service))
	if err != nil {
		return fmt.Errorf("%s serves no %s: %w", target.endpoint, target.service, err)
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return fmt.Errorf("%s is no service", target.service)
	}
	setID, err := toolSetID(target.service)
	if err != nil {
		return err
	}

	builder := aitool.NewBuilder(schema)
	nameSet := map[string]struct{}{}
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			continue
		}
		tool, err := builder.CreateTool(methodToolRequest(method))
		if err != nil {
			return fmt.Errorf("building the tool of %s: %w", method.FullName(), err)
		}
		if tool.GetDescription() == "" {
			// Reflection without source info carries no comments: the method's name is all there is to say.
			tool.Description = string(method.FullName())
		}
		tool.Labels = map[string]string{aipb.Labels.ToolSet.GetKey(): setID}
		// The builder names the tool as a model calls it: its title.
		tool.Title = tool.GetName()
		name := "tools/" + toolID(method)
		if service, ok := nameToService[name]; ok && service != target.service {
			return fmt.Errorf("%s of %s is %s's already", name, method.FullName(), service)
		}
		nameToService[name] = target.service
		if err := s.converge(ctx, name, tool); err != nil {
			return err
		}
		nameSet[name] = struct{}{}
	}
	return s.deleteStale(ctx, setID, nameSet)
}

// methodToolRequest returns the request building a method's tool: every field but the request_id its caller derives.
func methodToolRequest(method protoreflect.MethodDescriptor) *aienginepb.CreateToolRequest {
	var options []aitool.Option
	if method.Input().Fields().ByName("request_id") != nil {
		var paths []string
		fields := method.Input().Fields()
		for i := 0; i < fields.Len(); i++ {
			if field := fields.Get(i); field.Name() != "request_id" {
				paths = append(paths, string(field.Name()))
			}
		}
		options = append(options, aitool.WithFieldMask(pbfieldmask.FromPaths(paths...)))
	}
	return aitool.MethodToolRequest(aitool.NewMethod(string(method.FullName())), options...)
}

// converge writes a mirrored tool as the platform tool of a name: created if absent, its differing fields updated,
// untouched if equal.
func (s *Service) converge(ctx context.Context, name string, tool *aipb.Tool) error {
	createToolRequest := &aiservicepb.CreateToolRequest{
		Tool:      tool,
		ToolId:    strings.TrimPrefix(name, "tools/"),
		RequestId: uuid.NewV5(toolRequestIDNamespace, name).String(),
	}
	stored, err := s.aiServiceClient.CreateTool(ctx, createToolRequest)
	if err != nil {
		return fmt.Errorf("creating %s: %w", name, err)
	}
	var paths []string
	if stored.GetTitle() != tool.GetTitle() {
		paths = append(paths, "title")
	}
	if stored.GetDescription() != tool.GetDescription() {
		paths = append(paths, "description")
	}
	if !proto.Equal(stored.GetJsonSchema(), tool.GetJsonSchema()) {
		paths = append(paths, "json_schema")
	}
	if !maps.Equal(stored.GetAnnotations(), tool.GetAnnotations()) {
		paths = append(paths, "annotations")
	}
	if !maps.Equal(stored.GetLabels(), tool.GetLabels()) {
		paths = append(paths, "labels")
	}
	if len(paths) == 0 {
		return nil
	}
	update := proto.CloneOf(tool)
	update.Name = name
	update.Etag = stored.GetEtag()
	updateToolRequest := &aiservicepb.UpdateToolRequest{Tool: update, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}}
	if _, err := s.aiServiceClient.UpdateTool(ctx, updateToolRequest); err != nil {
		return fmt.Errorf("updating %s: %w", name, err)
	}
	return nil
}

// deleteStale deletes the platform tools of a set a pass did not write: methods the target no longer serves.
func (s *Service) deleteStale(ctx context.Context, setID string, nameSet map[string]struct{}) error {
	listToolsRequest := &aiservicepb.ListToolsRequest{
		Filter:   fmt.Sprintf(`labels.%q = %q`, aipb.Labels.ToolSet.GetKey(), setID),
		PageSize: 1000,
	}
	var stale []*aipb.Tool
	for tool, err := range aip.Iterator[*aipb.Tool](ctx, listToolsRequest, s.aiServiceClient.ListTools) {
		if err != nil {
			return fmt.Errorf("listing the tools of %s: %w", setID, err)
		}
		if _, ok := nameSet[tool.GetName()]; !ok {
			stale = append(stale, tool)
		}
	}
	for _, tool := range stale {
		deleteToolRequest := &aiservicepb.DeleteToolRequest{Name: tool.GetName(), Etag: tool.GetEtag(), AllowMissing: true}
		if _, err := s.aiServiceClient.DeleteTool(ctx, deleteToolRequest); err != nil {
			return fmt.Errorf("deleting %s: %w", tool.GetName(), err)
		}
		s.log.InfoContext(ctx, "deleted a tool its target no longer serves", "tool", tool.GetName())
	}
	return nil
}
