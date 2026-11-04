// agent-executor runs agents' turns locally: it dials an AgentService as an
// executor and gives the agents it is leased coding tools over a working directory.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/ai/agent_executor"
	"github.com/malonaz/core/go/ai/agent_executor/workspace"
	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/logging"
)

var opts struct {
	Logging *logging.Opts `group:"Logging" namespace:"logging" env-namespace:"LOGGING"`
	Certs   *certs.Opts   `group:"Certs" namespace:"certs" env-namespace:"CERTS"`

	Executor *agent_executor.Opts `group:"Executor" env-namespace:"EXECUTOR"`
	Workdir  string               `long:"workdir" env:"WORKDIR" description:"The directory the coding tools work in. Defaults to the current directory."`
	// Credentials, e.g. an API key or a bearer token, as the services' authentication expects them.
	Metadata map[string]string `long:"metadata" env:"METADATA" env-delim:"," description:"A key:value gRPC metadata pair sent with every call to both services, e.g. x-api-key:{key}. Repeatable."`

	AgentServiceGRPC *grpc.ClientOpts `group:"Agent Service GRPC (Client)" namespace:"agent-service-grpc" env-namespace:"AGENT_SERVICE_GRPC"`
	AiServiceGRPC    *grpc.ClientOpts `group:"Ai Service GRPC (Client)" namespace:"ai-service-grpc" env-namespace:"AI_SERVICE_GRPC"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("running", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := flags.Parse(&opts); err != nil {
		return err
	}
	if err := logging.Init(opts.Logging); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	workdir, err := filepath.Abs(opts.Workdir)
	if err != nil {
		return fmt.Errorf("resolving workdir: %w", err)
	}
	if info, err := os.Stat(workdir); err != nil {
		return fmt.Errorf("opening workdir: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("workdir %s is not a directory", workdir)
	}

	// One connection per endpoint, as both services may be served together.
	endpointToConnection := map[string]*grpc.Connection{}
	connect := func(clientOpts *grpc.ClientOpts) (*grpc.Connection, error) {
		if connection, ok := endpointToConnection[clientOpts.Endpoint()]; ok {
			return connection, nil
		}
		connection, err := grpc.NewConnection(clientOpts, opts.Certs, nil)
		if err != nil {
			return nil, err
		}
		for key, value := range opts.Metadata {
			connection.WithMetadata(key, value)
		}
		if err := connection.Connect(ctx); err != nil {
			return nil, err
		}
		endpointToConnection[clientOpts.Endpoint()] = connection
		return connection, nil
	}
	defer func() {
		for _, connection := range endpointToConnection {
			connection.Close()
		}
	}()
	agentServiceConnection, err := connect(opts.AgentServiceGRPC)
	if err != nil {
		return fmt.Errorf("connecting to agent service: %w", err)
	}
	aiServiceConnection, err := connect(opts.AiServiceGRPC)
	if err != nil {
		return fmt.Errorf("connecting to ai service: %w", err)
	}
	agentServiceClient := agentservicepb.NewAgentServiceClient(agentServiceConnection.Get())
	aiServiceClient := aiservicepb.NewAiServiceClient(aiServiceConnection.Get())

	executor, err := agent_executor.New(opts.Executor, agentServiceClient, aiServiceClient, workspace.NewTools(workdir)...)
	if err != nil {
		return fmt.Errorf("creating executor: %w", err)
	}
	slog.InfoContext(ctx, "running executor", "user", opts.Executor.User, "executor_id", opts.Executor.ExecutorID, "workdir", workdir)
	return executor.Run(ctx)
}
