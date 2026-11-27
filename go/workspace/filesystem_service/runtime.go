// Package filesystem_service stores the filesystems workspaces mount, tracks which workspaces
// mount them, and has the workspace agent on their node snapshot and purge them.
package filesystem_service

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Opts struct {
	AgentNamespace  string `long:"agent-namespace" env:"AGENT_NAMESPACE" required:"true" description:"The namespace the workspace agent's pods run in, one per node"`
	AgentPort       int    `long:"agent-port" env:"AGENT_PORT" default:"9090" description:"The workspace agent's gRPC port"`
	AgentDisableTLS bool   `long:"agent-disable-tls" env:"AGENT_DISABLE_TLS" description:"Dial the workspace agent in plaintext"`
}

type runtime struct {
	// nodes reaches the workspace agent of a node; set on start, or by tests.
	nodes nodes
}

func newRuntime(opts *Opts) (*runtime, error) {
	return &runtime{}, nil
}

func (s *Service) start(context.Context) (func(), error) {
	if s.nodes != nil {
		return func() {}, nil
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("loading in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}
	s.nodes = newAgentNodes(clientset, s.opts)
	return func() {}, nil
}
