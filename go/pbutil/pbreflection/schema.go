package pbreflection

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	aipv1 "github.com/malonaz/core/genproto/codegen/aip/v1"
)

var (
	commentOverrideRegexp = regexp.MustCompile(`@comment\(([^)]+)\)`)
)

type ResolveSchemaOption func(*resolveSchemaOptions)

type resolveSchemaOptions struct {
	cacheDir string
	cacheTTL time.Duration
}

type Schema struct {
	serviceSet map[string]struct{}
	Files      *protoregistry.Files
	Comments   map[string]string
}

func ResolveSchema(ctx context.Context, conn *grpc.ClientConn, opts ...ResolveSchemaOption) (*Schema, error) {
	options := &resolveSchemaOptions{}
	for _, opt := range opts {
		opt(options)
	}

	rpcClient := rpb.NewServerReflectionClient(conn)

	if options.cacheTTL <= 0 {
		data, err := resolve(ctx, rpcClient)
		if err != nil {
			return nil, err
		}
		return newSchema(data)
	}

	cacheKey := cacheKeyFor(conn.Target())

	if data := loadFromCache(cacheKey, options); data != nil {
		return newSchema(data)
	}

	data, err := resolve(ctx, rpcClient)
	if err != nil {
		return nil, err
	}

	saveToCache(cacheKey, data, options)

	return newSchema(data)
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

	// Step 2: Process @comment replacements.
	for fqn, comment := range comments {
		var replaceErr error
		newComment := commentOverrideRegexp.ReplaceAllStringFunc(comment, func(match string) string {
			m := commentOverrideRegexp.FindStringSubmatch(match)
			if m == nil {
				return match
			}
			if override, ok := comments[m[1]]; ok {
				return override
			}
			replaceErr = fmt.Errorf("@comment(%s) in %s: target not found", m[1], fqn)
			return match
		})
		if replaceErr != nil {
			return nil, replaceErr
		}
		comments[fqn] = newComment
	}

	// Step 3: Augment method comments based on AIP options
	augmentMethodComments(comments, files)

	return &Schema{
		serviceSet: serviceSet,
		Files:      files,
		Comments:   comments,
	}, nil
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

// In schema.go, add this method to Schema:

func (s *Schema) GetResource(resourceType string) *annotations.ResourceDescriptor {
	var result *annotations.ResourceDescriptor
	s.Files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
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

func (s *Schema) Services(yield func(protoreflect.ServiceDescriptor) bool) {
	s.Files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
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

func augmentMethodComments(comments map[string]string, files *protoregistry.Files) {
	messageFQNSeenSet := map[string]struct{}{}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
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
				// We do not update the same message twice.
				_, messageSeen := messageFQNSeenSet[string(input.FullName())]
				messageFQNSeenSet[string(input.FullName())] = struct{}{}

				methodFQN := string(method.FullName())
				var methodExtras []string

				// Filtering
				if proto.HasExtension(inputOpts, aipv1.E_Filtering) {
					if filtering := proto.GetExtension(inputOpts, aipv1.E_Filtering).(*aipv1.FilteringOptions); filtering != nil && len(filtering.Paths) > 0 {
						methodExtras = append(methodExtras, formatFilteringDoc(filtering.Paths))
						if !messageSeen {
							if field := input.Fields().ByName("filter"); field != nil {
								comments[string(field.FullName())] = fmt.Sprintf("Filter by: %s", strings.Join(filtering.Paths, ", "))
							}
						}
					}
				}

				// Ordering
				if proto.HasExtension(inputOpts, aipv1.E_Ordering) {
					if ordering := proto.GetExtension(inputOpts, aipv1.E_Ordering).(*aipv1.OrderingOptions); ordering != nil && len(ordering.Paths) > 0 {
						methodExtras = append(methodExtras, formatOrderingDoc(ordering.Paths, ordering.Default))
						if !messageSeen {
							if field := input.Fields().ByName("order_by"); field != nil {
								comments[string(field.FullName())] = fmt.Sprintf("Order by: %s (default: %s)", strings.Join(ordering.Paths, ", "), ordering.Default)
							}
						}
					}
				}

				// Pagination
				if proto.HasExtension(inputOpts, aipv1.E_Pagination) {
					if pagination := proto.GetExtension(inputOpts, aipv1.E_Pagination).(*aipv1.PaginationOptions); pagination != nil {
						methodExtras = append(methodExtras, formatPaginationDoc(pagination.DefaultPageSize))
						if !messageSeen {
							if field := input.Fields().ByName("page_size"); field != nil {
								comments[string(field.FullName())] = fmt.Sprintf("Page size (default: %d)", pagination.DefaultPageSize)
							}
						}
					}
				}

				// Updating
				if proto.HasExtension(inputOpts, aipv1.E_Update) {
					if update := proto.GetExtension(inputOpts, aipv1.E_Update).(*aipv1.UpdateOptions); update != nil && len(update.Paths) > 0 {
						methodExtras = append(methodExtras, formatUpdateDoc(update.Paths))
						if !messageSeen {
							if field := input.Fields().ByName("update_mask"); field != nil {
								comments[string(field.FullName())] = fmt.Sprintf("Updatable fields: %s", strings.Join(update.Paths, ", "))
							}
						}
					}
				}

				if len(methodExtras) > 0 {
					existing := comments[methodFQN]
					var newExtras []string
					for _, extra := range methodExtras {
						if !strings.Contains(existing, extra) {
							newExtras = append(newExtras, extra)
						}
					}
					if len(newExtras) > 0 {
						if existing != "" {
							comments[methodFQN] = existing + "\n\n" + strings.Join(newExtras, "\n\n")
						} else {
							comments[methodFQN] = strings.Join(newExtras, "\n\n")
						}
					}
				}
			}
		}
		return true
	})
}

func formatFilteringDoc(paths []string) string {
	var pathsStr string
	if len(paths) == 1 && paths[0] == "*" {
		pathsStr = "any field (except name)"
	} else {
		pathsStr = strings.Join(paths, ", ")
	}
	return fmt.Sprintf(`**Filtering (AIP-160)**
Supports filter expressions on: %s
Examples: 'name = "foo"', 'age > 21', 'status = "ACTIVE" AND created_time > "2024-01-01"'`, pathsStr)
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
