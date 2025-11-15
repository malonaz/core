package service

import (
	"context"
	"fmt"
	"github.com/malonaz/core/genproto/scheduler/v1/db"
	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

type Opts struct{}

type Runtime struct{}

func newRuntime(opts *Opts) (*Runtime, error) {
	return &Runtime{}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) { return func() {}, nil }

type Service struct {
	*Runtime
	opts               *Opts
	withServiceAccount func(context.Context) (context.Context, error)
	schedulerDBClient  *db.DB
}

// New instantiates and returns a new service.
func New(
	opts *Opts,
	schedulerDBClient *db.DB,

) (*Service, error) {
	runtime, err := newRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("instantiating runtime: %w", err)
	}
	return &Service{
		Runtime:           runtime,
		opts:              opts,
		schedulerDBClient: schedulerDBClient,
	}, nil
}

// Start this service. Returns clean up function.
func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) (context.Context, error)) (func(), error) {
	s.withServiceAccount = withServiceAccount
	ctxSA, err := withServiceAccount(ctx)
	if err != nil {
		return nil, err
	}
	return s.start(ctxSA)
}
