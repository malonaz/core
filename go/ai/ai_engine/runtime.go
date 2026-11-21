package ai_engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

type Opts struct {
	DefaultModel             string   `long:"default-model" env:"DEFAULT_MODEL" description:"The default model to use" required:"true"`
	FileDescriptorSetConfigs []string `long:"file-descriptor-set" env:"FILE_DESCRIPTOR_SET" description:"Use a local file descriptor set instead of a grpc reflection client. each item can be passed as 'filepath:fqn_service_name1,fqn_service_name2', e.g. 'path/to/fds.bin:user.user_service.v1.UserService,chat.chat_service.v1.ChatService'"`
	Files                    []string `long:"file" env:"FILES" env-delim:"," description:"Proto files (import paths) to resolve in addition to the reflected services, for messages tools generate that no RPC references. Repeatable"`
}

type runtime struct {
	reflectionServerOptions *reflection.ServerOptions
	files                   []string

	// Tool schemas are derived from proto descriptors fixed at build time, so an
	// identical request always yields an identical tool: caching on a
	// deterministic hash of the request avoids rebuilding schemas.
	toolCacheMutex sync.RWMutex
	hashToTool     map[string]*aipb.Tool
}

func newRuntime(opts *Opts) (*runtime, error) {
	var (
		aggregateFileDescriptorSet *descriptorpb.FileDescriptorSet
		serviceNames               []string
	)

	if len(opts.FileDescriptorSetConfigs) > 0 {
		aggregateFileDescriptorSet = &descriptorpb.FileDescriptorSet{}
	}
	fileDescriptorNameSet := map[string]struct{}{}
	for _, fileDescriptorSetConfig := range opts.FileDescriptorSetConfigs {
		parts := strings.SplitN(fileDescriptorSetConfig, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid file descriptor set config %q: expected format 'filepath:service1,service2'", fileDescriptorSetConfig)
		}
		fileDescriptorSetFilepath := parts[0]
		serviceNames = append(serviceNames, strings.Split(parts[1], ",")...)
		bytes, err := os.ReadFile(fileDescriptorSetFilepath)
		if err != nil {
			return nil, fmt.Errorf("reading file descriptor set file %q: %w", fileDescriptorSetFilepath, err)
		}
		fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
		if err := pbutil.Unmarshal(bytes, fileDescriptorSet); err != nil {
			return nil, fmt.Errorf("parsing file descriptor set file %q: %w", fileDescriptorSetFilepath, err)
		}
		for _, fileDescriptorProto := range fileDescriptorSet.GetFile() {
			if _, ok := fileDescriptorNameSet[fileDescriptorProto.GetName()]; ok {
				continue
			}
			fileDescriptorNameSet[fileDescriptorProto.GetName()] = struct{}{}
			aggregateFileDescriptorSet.File = append(aggregateFileDescriptorSet.File, fileDescriptorProto)
		}
	}

	var reflectionServerOptions *reflection.ServerOptions
	if aggregateFileDescriptorSet != nil {
		files, err := protodesc.NewFiles(aggregateFileDescriptorSet)
		if err != nil {
			return nil, fmt.Errorf("building file descriptor registry: %w", err)
		}
		types, err := pbreflection.NewTypesFromFiles(files)
		if err != nil {
			return nil, fmt.Errorf("new types from files: %w", err)
		}
		serviceInfoProvider, err := pbreflection.NewServiceInfoProvider(files, serviceNames)
		if err != nil {
			return nil, fmt.Errorf("instantiaing new service info provider: %w", err)
		}
		reflectionServerOptions = &reflection.ServerOptions{
			Services:           serviceInfoProvider,
			ExtensionResolver:  types,
			DescriptorResolver: files,
		}
	}

	return &runtime{
		reflectionServerOptions: reflectionServerOptions,
		files:                   opts.Files,
		hashToTool:              map[string]*aipb.Tool{},
	}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	if s.reflectionServerOptions != nil && s.serverReflectionClient != nil {
		return nil, fmt.Errorf("cannot use both grpc reflection client and file descriptor set")
	}
	if s.reflectionServerOptions == nil && s.serverReflectionClient == nil {
		return nil, fmt.Errorf("must provide either a grpc reflection client or a file descriptor set")
	}
	if s.reflectionServerOptions != nil {
		s.serverReflectionClient = pbreflection.NewServerReflectionClientInProc(*s.reflectionServerOptions)
	}
	return func() {}, nil
}

func (s *Service) getSchema(ctx context.Context) (*pbreflection.Schema, error) {
	return pbreflection.ResolveSchema(ctx, s.serverReflectionClient,
		pbreflection.WithMemCache("schema", time.Hour),
		pbreflection.WithFiles(s.files...),
	)
}

func (s *Service) CreateTool(ctx context.Context, request *pb.CreateToolRequest) (*aipb.Tool, error) {
	hash, err := hashRequest(request)
	if err != nil {
		return nil, err
	}
	s.toolCacheMutex.RLock()
	cachedTool, ok := s.hashToTool[hash]
	s.toolCacheMutex.RUnlock()
	if ok {
		return proto.CloneOf(cachedTool), nil
	}

	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	tool, err := aitool.NewBuilder(schema).CreateTool(request)
	if err != nil {
		return nil, err
	}
	s.toolCacheMutex.Lock()
	s.hashToTool[hash] = tool
	s.toolCacheMutex.Unlock()
	return proto.CloneOf(tool), nil
}

// hashRequest produces a stable hash of a request. Deterministic marshaling is
// required because protobuf wire output is otherwise unstable across map
// iteration order.
func hashRequest(message proto.Message) (string, error) {
	marshalOptions := proto.MarshalOptions{Deterministic: true}
	bytes, err := marshalOptions.Marshal(message)
	if err != nil {
		return "", status.Errorf(codes.Internal, "marshaling request: %v", err).Err()
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) BatchCreateTools(ctx context.Context, request *pb.BatchCreateToolsRequest) (*pb.BatchCreateToolsResponse, error) {
	tools := make([]*aipb.Tool, len(request.GetRequests()))
	for i, createToolRequest := range request.GetRequests() {
		tool, err := s.CreateTool(ctx, createToolRequest)
		if err != nil {
			return nil, err
		}
		tools[i] = tool
	}
	return &pb.BatchCreateToolsResponse{Tools: tools}, nil
}

func (s *Service) ParseToolCall(ctx context.Context, request *pb.ParseToolCallRequest) (*pb.ParseToolCallResponse, error) {
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	schemaBuilder := pbjson.NewSchemaBuilder(schema)
	return aitool.ParseToolCall(schemaBuilder, request.GetToolCall(), request.GetToolSets())
}

func (s *Service) CreateDiscoveryTool(ctx context.Context, request *pb.CreateDiscoveryToolRequest) (*aipb.Tool, error) {
	return aitool.CreateDiscoveryTool(&aitool.CreateDiscoveryToolRequest{
		Name:        request.Name,
		Description: request.Description,
		Tools:       request.Tools,
	}), nil
}

func (s *Service) CreateServiceToolSet(ctx context.Context, request *pb.CreateServiceToolSetRequest) (*aipb.ToolSet, error) {
	schema, err := s.getSchema(ctx)
	if err != nil {
		return nil, err
	}
	return aitool.NewBuilder(schema).CreateServiceToolSet(request)
}
