package agent_service

import (
	"context"
	"fmt"
	"github.com/malonaz/core/gengo/agent/agent_service/rpc"
	"github.com/malonaz/core/gengo/agent/store"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/postgres"
	"log/slog"
)

type Service struct {
	*runtime
	*rpc.AgentServiceServer
	log                 *slog.Logger
	opts                *Opts
	withServiceAccount  func(context.Context) context.Context
	agentPostgresClient *postgres.Client
	agentPostgresStore  *store.Store

	aiServiceClient aiservicepb.AiServiceClient
}

func (s *Service) WithLogger(logger *slog.Logger) *Service {
	s.log = logger
	return s
}

func New(
	opts *Opts,
	agentPostgresClient *postgres.Client,
	agentPostgresStore *store.Store,

	aiServiceClient aiservicepb.AiServiceClient,

) (*Service, error) {
	runtime, err := newRuntime(opts)
	if err != nil {
		return nil, fmt.Errorf("instantiating runtime: %w", err)
	}
	return &Service{
		runtime:             runtime,
		AgentServiceServer:  rpc.NewAgentServiceServer(agentPostgresStore),
		log:                 slog.Default(),
		opts:                opts,
		agentPostgresClient: agentPostgresClient,
		agentPostgresStore:  agentPostgresStore,

		aiServiceClient: aiServiceClient,
	}, nil
}

func (s *Service) Start(ctx context.Context, withServiceAccount func(context.Context) context.Context) (func(), error) {
	if withServiceAccount != nil {
		s.withServiceAccount = withServiceAccount
		ctx = withServiceAccount(ctx)
	}
	if err := s.AgentServiceServer.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting agent-service server: %w", err)
	}
	return s.start(ctx)
}
