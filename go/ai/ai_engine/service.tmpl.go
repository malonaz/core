package ai_engine

import (
	"context"
	"fmt"
	serverreflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"log/slog"
)

type Service struct {
	*runtime
	log                *slog.Logger
	opts               *Opts
	withServiceAccount func(context.Context) context.Context

	serverReflectionClient serverreflectionpb.ServerReflectionClient
}

func (s *Service) WithLogger(logger *slog.Logger) *Service {
	s.log = logger
	return s
}

func New(
	opts *Opts,

	serverReflectionClient serverreflectionpb.ServerReflectionClient,

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
	}, nil
}

func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) context.Context) (func(), error) {
	if withServiceAccount != nil {
		s.withServiceAccount = withServiceAccount
		ctx = withServiceAccount(ctx)
	}
	return s.start(ctx)
}
