package pbreflection

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
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

	comments := make(map[string]string)
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		extractComments(comments, fd)
		return true
	})

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
	for i := 0; i < values.Len(); i++ {
		storeComment(comments, locs, values.Get(i))
	}
}

func storeComment(comments map[string]string, locs protoreflect.SourceLocations, desc protoreflect.Descriptor) {
	loc := locs.ByDescriptor(desc)
	comment := strings.TrimSpace(loc.LeadingComments)
	if comment == "" {
		comment = strings.TrimSpace(loc.TrailingComments)
	}
	if comment != "" {
		comments[string(desc.FullName())] = comment
	}
}
