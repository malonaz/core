package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/ai/ai_engine/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// AIEngineClient is the subset of the AI engine client the registry depends on.
type AIEngineClient interface {
	CreateTool(ctx context.Context, request *pb.CreateToolRequest, options ...grpc.CallOption) (*aipb.Tool, error)
}

// Registry caches tools built by the AI engine. Tool schemas are derived from
// proto descriptors fixed at build time, so an identical request always yields
// an identical tool: caching on a deterministic hash of the request avoids a
// round trip per generation.
type Registry struct {
	aiEngineClient AIEngineClient

	mutex      sync.RWMutex
	hashToTool map[string]*aipb.Tool
}

func NewRegistry(aiEngineClient AIEngineClient) *Registry {
	return &Registry{
		aiEngineClient: aiEngineClient,
		hashToTool:     map[string]*aipb.Tool{},
	}
}

func (r *Registry) CreateTool(ctx context.Context, createToolRequest *pb.CreateToolRequest) (*aipb.Tool, error) {
	hash, err := hashRequest(createToolRequest)
	if err != nil {
		return nil, err
	}

	r.mutex.RLock()
	tool, ok := r.hashToTool[hash]
	r.mutex.RUnlock()
	if ok {
		return tool, nil
	}

	tool, err = r.aiEngineClient.CreateTool(ctx, createToolRequest)
	if err != nil {
		return nil, status.FromError(err, "creating tool").Err()
	}

	r.mutex.Lock()
	r.hashToTool[hash] = tool
	r.mutex.Unlock()

	return tool, nil
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
