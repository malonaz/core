package ai_service

import (
	"context"
	"fmt"
	"log/slog"
)

type Service struct {
	*runtime
	*codegen
}

type codegen struct {
	log                *slog.Logger
	opts               *Opts
	withServiceAccount func(context.Context) context.Context
}

func (s *Service) WithLogger(logger *slog.Logger) *Service {
	s.log = logger
	return s
}

// New instantiates and returns a new service.
func New(
	opts *Opts,

) (*Service, error) {
	runtime, err := newRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("instantiating runtime: %w", err)
	}
	codegen := &codegen{
		log:  slog.Default(),
		opts: opts,
	}
	return &Service{
		runtime: runtime,
		codegen: codegen,
	}, nil
}

// Start this service. Returns clean up function.
func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) context.Context) (func(), error) {
	s.withServiceAccount = withServiceAccount
	ctxSA := withServiceAccount(ctx)
	return s.start(ctxSA)
}
