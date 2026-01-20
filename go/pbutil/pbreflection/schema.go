package pbreflection

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/huandu/xstrings"
	"google.golang.org/genproto/googleapis/api/annotations"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	gatewaypb "github.com/malonaz/core/genproto/codegen/gateway/v1"
	"github.com/malonaz/core/go/aip"
)

type CommentStyle int
type StandardMethodType string

const (
	CommentStyleFirstLine CommentStyle = iota
	CommentStyleMultiline
	CommentStyleSingleLine

	StandardMethodTypeUnspecified StandardMethodType = ""
	StandardMethodTypeCreate      StandardMethodType = "Create"
	StandardMethodTypeGet         StandardMethodType = "Get"
	StandardMethodTypeBatchGet    StandardMethodType = "BatchGet"
	StandardMethodTypeUpdate      StandardMethodType = "Update"
	StandardMethodTypeDelete      StandardMethodType = "Delete"
	StandardMethodTypeList        StandardMethodType = "List"
)

var (
	commentOverrideRegexp   = regexp.MustCompile(`@comment\(([^)]+)\)`)
	resourcePatternWildcard = regexp.MustCompile(`\{[^}]+\}`)
)

type ResolveSchemaOption func(*resolveSchemaOptions)

type resolveSchemaOptions struct {
	cacheKey string
	cacheDir string
	cacheTTL time.Duration
}

func (o *resolveSchemaOptions) memCache() bool {
	return o.cacheKey != "" && o.cacheDir == ""
}

func (o *resolveSchemaOptions) dirCache() bool {
	return o.cacheKey != "" && o.cacheDir != ""
}

type Schema struct {
	files                                     *protoregistry.Files
	serviceSet                                map[string]struct{}
	comments                                  map[string]string
	resourceTypeToMessageDescriptor           map[string]protoreflect.MessageDescriptor
	methodFullNameToStandardMethodType        map[protoreflect.FullName]StandardMethodType
	methodFullNameToResourceMessageDescriptor map[protoreflect.FullName]protoreflect.MessageDescriptor
}

func ResolveSchema(ctx context.Context, reflectionClient rpb.ServerReflectionClient, opts ...ResolveSchemaOption) (*Schema, error) {
	options := &resolveSchemaOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Try mem cache first (returns Schema directly).
	if options.memCache() {
		if schema := loadFromMemCache(options); schema != nil {
			return schema, nil
		}
	}

	// Try disk cache.
	var data *schemaData
	if options.dirCache() {
		data = loadFromFileCache(options)
	}

	// Fallback to resolution.
	if data == nil {
		var err error
		data, err = resolve(ctx, reflectionClient)
		if err != nil {
			return nil, err
		}
		if options.dirCache() {
			saveToFileCache(data, options)
		}
	}

	schema, err := newSchema(data)
	if err != nil {
		return nil, err
	}

	if options.memCache() {
		saveToMemCache(schema, options)
	}
	return schema, nil
}

func (s *Schema) Services(yield func(protoreflect.ServiceDescriptor) bool) {
	s.files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			if _, ok := s.serviceSet[string(svc.FullName())]; ok {
				if !yield(svc) {
					return false
				}
			}
		}
		return true
	})
}

func (s *Schema) GetComment(name protoreflect.FullName, style CommentStyle) string {
	c, ok := s.comments[string(name)]
	if !ok {
		return ""
	}
	switch style {
	case CommentStyleFirstLine:
		if idx := strings.Index(c, "\n"); idx != -1 {
			return c[:idx]
		}
		return c
	case CommentStyleMultiline:
		return c
	case CommentStyleSingleLine:
		return strings.ReplaceAll(c, "\n", " ")
	default:
		return c
	}
}

func (s *Schema) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return s.files.FindDescriptorByName(name)
}

// GetStandardMethodType returns the standard method type for a method, or empty string if not a standard method.
func (s *Schema) GetStandardMethodType(methodFullName protoreflect.FullName) StandardMethodType {
	// Check for proxy annotation.
	desc, err := s.files.FindDescriptorByName(methodFullName)
	if err != nil {
		return StandardMethodTypeUnspecified
	}
	method, ok := desc.(protoreflect.MethodDescriptor)
	if !ok {
		return StandardMethodTypeUnspecified
	}
	methodOpts := method.Options()
	if methodOpts == nil || !proto.HasExtension(methodOpts, gatewaypb.E_Opts) {
		return s.methodFullNameToStandardMethodType[methodFullName]
	}
	opts := proto.GetExtension(methodOpts, gatewaypb.E_Opts).(*gatewaypb.HandlerOpts)
	if opts.GetProxy() == "" {
		return StandardMethodTypeUnspecified
	}
	return s.methodFullNameToStandardMethodType[protoreflect.FullName(opts.GetProxy())]
}

func (s *Schema) GetResourceDescriptor(resourceType string) *annotations.ResourceDescriptor {
	var result *annotations.ResourceDescriptor
	s.files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		messages := fd.Messages()
		for i := 0; i < messages.Len(); i++ {
			msg := messages.Get(i)
			opts := msg.Options()
			if opts == nil {
				continue
			}
			if !proto.HasExtension(opts, annotations.E_Resource) {
				continue
			}
			res := proto.GetExtension(opts, annotations.E_Resource).(*annotations.ResourceDescriptor)
			if res.GetType() == resourceType {
				result = res
				return false
			}
		}
		return true
	})
	return result
}

func newSchema(data *schemaData) (*Schema, error) {
	files := new(protoregistry.Files)
	fdMap := make(map[string]*descriptorpb.FileDescriptorProto)
	for _, fd := range data.FileDescriptors {
		fdMap[fd.GetName()] = fd
	}

	var registerFile func(*descriptorpb.FileDescriptorProto) error
	registerFile = func(fdProto *descriptorpb.FileDescriptorProto) error {
		if _, err := files.FindFileByPath(fdProto.GetName()); err == nil {
			return nil
		}
		for _, dep := range fdProto.GetDependency() {
			if depProto, ok := fdMap[dep]; ok {
				if err := registerFile(depProto); err != nil {
					return err
				}
			}
		}
		fd, err := protodesc.NewFile(fdProto, files)
		if err != nil {
			return fmt.Errorf("creating file descriptor for %s: %w", fdProto.GetName(), err)
		}
		return files.RegisterFile(fd)
	}

	for _, fdProto := range data.FileDescriptors {
		if err := registerFile(fdProto); err != nil {
			return nil, err
		}
	}

	serviceSet := make(map[string]struct{})
	for _, svc := range data.ServiceSet {
		serviceSet[svc] = struct{}{}
	}

	// Step 1: extract comments.
	comments := make(map[string]string)
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		extractComments(comments, fd)
		return true
	})

	// Step 2: parse resource messages.
	resourceTypeToMessageDescriptor := map[string]protoreflect.MessageDescriptor{}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		messages := fd.Messages()
		for i := 0; i < messages.Len(); i++ {
			msg := messages.Get(i)
			opts := msg.Options()
			if opts == nil {
				continue
			}
			if !proto.HasExtension(opts, annotations.E_Resource) {
				continue
			}
			res := proto.GetExtension(opts, annotations.E_Resource).(*annotations.ResourceDescriptor)
			resourceTypeToMessageDescriptor[res.GetType()] = msg
		}
		return true
	})

	// Define the schema.
	schema := &Schema{
		files:                              files,
		serviceSet:                         serviceSet,
		resourceTypeToMessageDescriptor:    resourceTypeToMessageDescriptor,
		methodFullNameToStandardMethodType: map[protoreflect.FullName]StandardMethodType{},
		methodFullNameToResourceMessageDescriptor: map[protoreflect.FullName]protoreflect.MessageDescriptor{},
		comments: comments,
	}

	// Step 3: build the standard method types.
	if err := schema.buildStandardMethodTypes(); err != nil {
		return nil, fmt.Errorf("building standard method types: %w", err)
	}

	// Step 4: Augment method comments based on AIP options
	if err := schema.augmentMethodComments(); err != nil {
		return nil, fmt.Errorf("augmenting method comments: %w", err)
	}

	// Step 5: Process @comment replacements.
	for fqn, comment := range schema.comments {
		var replaceErr error
		newComment := commentOverrideRegexp.ReplaceAllStringFunc(comment, func(match string) string {
			m := commentOverrideRegexp.FindStringSubmatch(match)
			if m == nil {
				return match
			}
			targetFQN := protoreflect.FullName(m[1])

			// Propagate standard method type from referenced method.
			if methodType := schema.methodFullNameToStandardMethodType[targetFQN]; methodType != StandardMethodTypeUnspecified {
				schema.methodFullNameToStandardMethodType[protoreflect.FullName(fqn)] = methodType
			}

			if override, ok := schema.comments[m[1]]; ok {
				return override
			}
			replaceErr = fmt.Errorf("@comment(%s) in %s: target not found", m[1], fqn)
			return match
		})
		if replaceErr != nil {
			return nil, replaceErr
		}
		schema.comments[fqn] = newComment
	}

	return schema, nil
}

func resolve(ctx context.Context, client rpb.ServerReflectionClient) (*schemaData, error) {
	stream, err := client.ServerReflectionInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating reflection stream: %w", err)
	}
	defer stream.CloseSend()

	services, err := listServices(stream)
	if err != nil {
		return nil, err
	}

	fdProtos := make(map[string]*descriptorpb.FileDescriptorProto)
	for _, svc := range services {
		if err := fetchFileDescriptors(stream, fdProtos, svc); err != nil {
			return nil, err
		}
	}

	data := &schemaData{ServiceSet: services}
	for _, fd := range fdProtos {
		data.FileDescriptors = append(data.FileDescriptors, fd)
	}
	return data, nil
}

func listServices(stream rpb.ServerReflection_ServerReflectionInfoClient) ([]string, error) {
	if err := stream.Send(&rpb.ServerReflectionRequest{
		MessageRequest: &rpb.ServerReflectionRequest_ListServices{},
	}); err != nil {
		return nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var services []string
	for _, svc := range resp.GetListServicesResponse().GetService() {
		services = append(services, svc.GetName())
	}
	return services, nil
}

func fetchFileDescriptors(stream rpb.ServerReflection_ServerReflectionInfoClient, fdProtos map[string]*descriptorpb.FileDescriptorProto, symbol string) error {
	if err := stream.Send(&rpb.ServerReflectionRequest{
		MessageRequest: &rpb.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: symbol,
		},
	}); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	for _, fdBytes := range resp.GetFileDescriptorResponse().GetFileDescriptorProto() {
		fdProto := new(descriptorpb.FileDescriptorProto)
		if err := proto.Unmarshal(fdBytes, fdProto); err != nil {
			return fmt.Errorf("unmarshaling file descriptor: %w", err)
		}
		fdProtos[fdProto.GetName()] = fdProto
	}
	return nil
}

// Call this in newSchema after building the files registry:
func (s *Schema) buildStandardMethodTypes() error {
	var err error
	s.files.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		services := fileDescriptor.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				methodOptions := method.Options()
				if methodOptions == nil || !proto.HasExtension(methodOptions, aippb.E_StandardMethod) {
					continue
				}
				standardMethod := proto.GetExtension(methodOptions, aippb.E_StandardMethod).(*aippb.StandardMethod)
				if standardMethod.GetResource() == "" {
					err = fmt.Errorf("method %s has incomplete standard method annotation", method.FullName())
					return false
				}
				resourceDesc := s.GetResourceDescriptor(standardMethod.GetResource())
				if resourceDesc == nil {
					err = fmt.Errorf("could not find resource descriptor %s for method %s", standardMethod.GetResource(), method.FullName())
					return false
				}
				if resourceDesc.GetSingular() == "" || resourceDesc.GetPlural() == "" {
					err = fmt.Errorf("resource descriptor %s is missing singular or plural values", standardMethod.GetResource())
					return false
				}
				resource, ok := s.resourceTypeToMessageDescriptor[resourceDesc.GetType()]
				if !ok {
					err = fmt.Errorf("cannot find resource message for resource descriptor %s ", standardMethod.GetResource())
					return false
				}
				s.methodFullNameToResourceMessageDescriptor[method.FullName()] = resource

				methodName := string(method.Name())
				singular := xstrings.ToPascalCase(resourceDesc.GetSingular())
				plural := xstrings.ToPascalCase(resourceDesc.GetPlural())
				var methodType StandardMethodType
				switch methodName {
				case string(StandardMethodTypeCreate) + singular:
					methodType = StandardMethodTypeCreate
				case string(StandardMethodTypeGet) + singular:
					methodType = StandardMethodTypeGet
				case string(StandardMethodTypeBatchGet) + plural:
					methodType = StandardMethodTypeBatchGet
				case string(StandardMethodTypeUpdate) + singular:
					methodType = StandardMethodTypeUpdate
				case string(StandardMethodTypeDelete) + singular:
					methodType = StandardMethodTypeDelete
				case string(StandardMethodTypeList) + plural:
					methodType = StandardMethodTypeList
				default:
					err = fmt.Errorf("method %s has standard annotation but does not match any of the standard method types", method.FullName())
					return false
				}
				s.methodFullNameToStandardMethodType[method.FullName()] = methodType

			}
		}
		return true
	})

	return err
}

func extractComments(comments map[string]string, fd protoreflect.FileDescriptor) {
	locs := fd.SourceLocations()
	storeComment(comments, locs, fd)

	messages := fd.Messages()
	for i := 0; i < messages.Len(); i++ {
		extractMessageComments(comments, locs, messages.Get(i))
	}

	services := fd.Services()
	for i := 0; i < services.Len(); i++ {
		svc := services.Get(i)
		storeComment(comments, locs, svc)
		methods := svc.Methods()
		for j := 0; j < methods.Len(); j++ {
			storeComment(comments, locs, methods.Get(j))
		}
	}

	enums := fd.Enums()
	for i := 0; i < enums.Len(); i++ {
		extractEnumComments(comments, locs, enums.Get(i))
	}
}

func extractMessageComments(comments map[string]string, locs protoreflect.SourceLocations, msg protoreflect.MessageDescriptor) {
	storeComment(comments, locs, msg)
	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		storeComment(comments, locs, fields.Get(i))
	}
	nested := msg.Messages()
	for i := 0; i < nested.Len(); i++ {
		extractMessageComments(comments, locs, nested.Get(i))
	}
	enums := msg.Enums()
	for i := 0; i < enums.Len(); i++ {
		extractEnumComments(comments, locs, enums.Get(i))
	}
}

func extractEnumComments(comments map[string]string, locs protoreflect.SourceLocations, enum protoreflect.EnumDescriptor) {
	storeComment(comments, locs, enum)
	values := enum.Values()
	var valueNames []string
	for i := 0; i < values.Len(); i++ {
		storeComment(comments, locs, values.Get(i))
		valueNames = append(valueNames, string(values.Get(i).Name()))
	}
	enumFQN := string(enum.FullName())
	existing := comments[enumFQN]
	valuesDoc := "Values: " + strings.Join(valueNames, ", ")
	if existing != "" {
		comments[enumFQN] = existing + "\n" + valuesDoc
	} else {
		comments[enumFQN] = valuesDoc
	}
}

func storeComment(comments map[string]string, locs protoreflect.SourceLocations, desc protoreflect.Descriptor) {
	loc := locs.ByDescriptor(desc)
	comment := trimCommentLines(loc.LeadingComments)
	if comment == "" {
		comment = trimCommentLines(loc.TrailingComments)
	}
	if comment != "" {
		comments[string(desc.FullName())] = comment
	}
}

func trimCommentLines(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (s *Schema) augmentMethodComments() error {
	var err error

	messageFQNSeenSet := map[string]struct{}{}
	s.files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				input := method.Input()
				inputOpts := input.Options()
				if inputOpts == nil {
					continue
				}
				// We don't process functions with overridable comments.
				methodFQN := string(method.FullName())
				if existing := s.comments[methodFQN]; commentOverrideRegexp.MatchString(existing) {
					continue
				}

				// We do not update the same message twice.
				standardMethodType := s.GetStandardMethodType(method.FullName())
				_, messageSeen := messageFQNSeenSet[string(input.FullName())]
				messageFQNSeenSet[string(input.FullName())] = struct{}{}

				var methodExtras []string

				// Parent field wildcard support
				if !messageSeen {
					if field := input.Fields().ByName("parent"); field != nil {
						fieldOpts := field.Options()
						if fieldOpts != nil && proto.HasExtension(fieldOpts, annotations.E_ResourceReference) {
							ref := proto.GetExtension(fieldOpts, annotations.E_ResourceReference).(*annotations.ResourceReference)
							if ref.GetType() != "" {
								if resourceDesc := s.GetResourceDescriptor(ref.GetType()); len(resourceDesc.GetPattern()) > 0 {
									pattern := resourceDesc.GetPattern()[0]
									wildcardPattern := resourcePatternWildcard.ReplaceAllString(pattern, "-")
									s.comments[string(field.FullName())] += fmt.Sprintf("\nWildcard '-' can be used for any segment: e.g. %s", wildcardPattern)
								}
							}
						}
					}
				}

				// Filtering
				if proto.HasExtension(inputOpts, aippb.E_Filtering) {
					if filtering := proto.GetExtension(inputOpts, aippb.E_Filtering).(*aippb.FilteringOptions); filtering != nil && len(filtering.Paths) > 0 {
						resourceMsg := s.methodFullNameToResourceMessageDescriptor[method.FullName()]
						methodExtras = append(methodExtras, formatFilteringDoc(resourceMsg, filtering.Paths))
						if !messageSeen {
							paths := filtering.GetPaths()
							if standardMethodType != StandardMethodTypeUnspecified {
								resourceMessageDescriptor, ok := s.methodFullNameToResourceMessageDescriptor[method.FullName()]
								if !ok {
									err = fmt.Errorf("could not find resource message descriptor for method %s", method.FullName())
									return false
								}
								var tree *aip.Tree
								tree, err = aip.BuildResourceTreeFromDescriptor(
									resourceMessageDescriptor,
									aip.WithAllowedPaths(filtering.Paths),
									aip.WithTransformNestedPath(),
									aip.WithMaxDepth(3),
								)
								if err != nil {
									return false
								}
								paths = tree.AllowedPaths()
							}

							if field := input.Fields().ByName("filter"); field != nil {
								s.comments[string(field.FullName())] = fmt.Sprintf("Filter by: %s", strings.Join(paths, ", "))
							}
						}
					}
				}

				// Ordering
				if proto.HasExtension(inputOpts, aippb.E_Ordering) {
					if ordering := proto.GetExtension(inputOpts, aippb.E_Ordering).(*aippb.OrderingOptions); ordering != nil && len(ordering.Paths) > 0 {
						methodExtras = append(methodExtras, formatOrderingDoc(ordering.Paths, ordering.Default))
						if !messageSeen {
							if field := input.Fields().ByName("order_by"); field != nil {
								s.comments[string(field.FullName())] = fmt.Sprintf("Order by: %s (default: %s)", strings.Join(ordering.Paths, ", "), ordering.Default)
							}
						}
					}
				}

				// Pagination
				if proto.HasExtension(inputOpts, aippb.E_Pagination) {
					if pagination := proto.GetExtension(inputOpts, aippb.E_Pagination).(*aippb.PaginationOptions); pagination != nil {
						methodExtras = append(methodExtras, formatPaginationDoc(pagination.DefaultPageSize))
						if !messageSeen {
							if field := input.Fields().ByName("page_size"); field != nil {
								s.comments[string(field.FullName())] = fmt.Sprintf("Page size (default: %d)", pagination.DefaultPageSize)
							}
						}
					}
				}

				// Updating
				if proto.HasExtension(inputOpts, aippb.E_Update) {
					if update := proto.GetExtension(inputOpts, aippb.E_Update).(*aippb.UpdateOptions); update != nil && len(update.Paths) > 0 {
						methodExtras = append(methodExtras, formatUpdateDoc(update.Paths))
						if !messageSeen {
							if field := input.Fields().ByName("update_mask"); field != nil {
								s.comments[string(field.FullName())] = fmt.Sprintf("Updatable fields: %s", strings.Join(update.Paths, ", "))
							}
						}
					}
				}

				if len(methodExtras) > 0 {
					existing := s.comments[methodFQN]
					var newExtras []string
					for _, extra := range methodExtras {
						if !strings.Contains(existing, extra) {
							newExtras = append(newExtras, extra)
						}
					}
					if len(newExtras) > 0 {
						if existing != "" {
							s.comments[methodFQN] = existing + "\n\n" + strings.Join(newExtras, "\n\n")
						} else {
							s.comments[methodFQN] = strings.Join(newExtras, "\n\n")
						}
					}
				}
			}
		}
		return true
	})

	return err
}

func formatFilteringDoc(resourceMsg protoreflect.MessageDescriptor, paths []string) string {
	var examples []string

	if resourceMsg != nil {
		var hasString, hasBool, hasEnum, hasTimestamp bool

		pathSet := make(map[string]struct{})
		for _, p := range paths {
			pathSet[p] = struct{}{}
		}

		fields := resourceMsg.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			name := string(field.Name())
			if _, ok := pathSet[name]; !ok {
				continue
			}

			switch field.Kind() {
			case protoreflect.StringKind:
				if !hasString {
					examples = append(examples, fmt.Sprintf(`%s = "example"`, name))
					hasString = true
				}
			case protoreflect.BoolKind:
				if !hasBool {
					examples = append(examples, name)
					hasBool = true
				}
			case protoreflect.EnumKind:
				if !hasEnum {
					enumVals := field.Enum().Values()
					if enumVals.Len() > 1 {
						examples = append(examples, fmt.Sprintf(`%s = %s`, name, enumVals.Get(1).Name()))
					}
					hasEnum = true
				}
			case protoreflect.MessageKind:
				if field.Message().FullName() == "google.protobuf.Timestamp" && !hasTimestamp {
					examples = append(examples, fmt.Sprintf(`%s > "2024-01-01T00:00:00Z"`, name))
					hasTimestamp = true
				}
			}
		}
	}

	var hasNested bool
	for _, p := range paths {
		if strings.Contains(p, ".") && !hasNested {
			examples = append(examples, fmt.Sprintf(`%s = "value"`, p))
			hasNested = true
			break
		}
	}

	if len(examples) == 0 {
		return fmt.Sprintf(`**Filtering (AIP-160)**
Filterable fields: %s
Note: boolean fields use 'field_name' (true) or 'NOT field_name' (false)`, strings.Join(paths, ", "))
	}

	exampleStr := "Examples: " + strings.Join(examples, ", ")
	return fmt.Sprintf(`**Filtering (AIP-160)**
%s
Note: boolean fields use 'field_name' (true) or 'NOT field_name' (false)`, exampleStr)
}

func formatOrderingDoc(paths []string, defaultOrder string) string {
	return fmt.Sprintf(`**Ordering (AIP-132)**
Order by: %s
Default: %s
Use 'field desc' for descending order.`, strings.Join(paths, ", "), defaultOrder)
}

func formatPaginationDoc(defaultSize uint32) string {
	return fmt.Sprintf(`**Pagination (AIP-158)**
Default page size: %d
Use page_token from response to fetch next page.`, defaultSize)
}

func formatUpdateDoc(paths []string) string {
	return fmt.Sprintf(`**Field Mask (AIP-134)**
Updatable fields: %s
Must use the update mask to specify which fields to update.`, strings.Join(paths, ", "))
}
