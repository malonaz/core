package ai_engine

import (
	"context"
	"fmt"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	serverreflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"log/slog"
)

type Service struct {
	*runtime
	log                *slog.Logger
	opts               *Opts
	withServiceAccount func(context.Context) context.Context

	serverReflectionClient serverreflectionpb.ServerReflectionClient

	aiServiceClient aiservicepb.AiServiceClient
}

func (s *Service) WithLogger(logger *slog.Logger) *Service {
	s.log = logger
	return s
}

// New instantiates and returns a new service.
func New(
	opts *Opts,

	serverReflectionClient serverreflectionpb.ServerReflectionClient,

	aiServiceClient aiservicepb.AiServiceClient,

) (*Service, error) {
	runtime, err := newRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("instantiating runtime: %w", err)
	}
	return &Service{
		runtime: runtime,
		log:     slog.Default(),
		opts:    opts,

		serverReflectionClient: serverReflectionClient,

		aiServiceClient: aiServiceClient,
	}, nil
}

func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) context.Context) (func(), error) {
	if withServiceAccount != nil {
		s.withServiceAccount = withServiceAccount
		ctx = withServiceAccount(ctx)
	}
	return s.start(ctx)
}
