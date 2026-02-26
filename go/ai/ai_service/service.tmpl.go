package ai_service

import (
	"context"
	"fmt"
	"github.com/malonaz/core/gengo/ai/ai_service/rpc"
	"github.com/malonaz/core/gengo/ai/store"
	"log/slog"
)

type Service struct {
	*runtime
	// Embedded codegen.
	*rpc.AiServiceServer
	log                *slog.Logger
	opts               *Opts
	withServiceAccount func(context.Context) context.Context
	aiPostgresStore    *store.Store
}

func (s *Service) WithLogger(logger *slog.Logger) *Service {
	s.log = logger
	return s
}

// New instantiates and returns a new service.
func New(
	opts *Opts,
	aiPostgresStore *store.Store,

) (*Service, error) {
	runtime, err := newRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("instantiating runtime: %w", err)
	}
	return &Service{
		runtime:         runtime,
		AiServiceServer: rpc.NewAiServiceServer(aiPostgresStore),
		log:             slog.Default(),
		opts:            opts,
		aiPostgresStore: aiPostgresStore,
	}, nil
}

// Start this service. Returns clean up function.
func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) context.Context) (func(), error) {
	if withServiceAccount != nil {
		s.withServiceAccount = withServiceAccount
		ctxSA := withServiceAccount(ctx)
		return s.start(ctxSA)
	}
	return s.start(ctx)
}
