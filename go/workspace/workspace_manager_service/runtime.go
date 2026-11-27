// Package workspace_manager_service runs workspaces: it creates and deletes their pods, attaches
// the filesystems they mount, snapshots them, and stops the idle ones.
package workspace_manager_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	workspaceservicepb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/routine"
	"github.com/malonaz/core/go/scheduler/longrunning"
	"github.com/malonaz/core/go/workspace/pod"
)

// createSnapshotMethod is the operation SnapshotWorkspace waits on, one per mount.
const createSnapshotMethod = "/malonaz.workspace.filesystem_service.v1.FilesystemService/CreateSnapshot"

// reapInterval is how often the reaper looks for pods to stop.
const reapInterval = time.Minute

type Opts struct {
	Namespace      string        `long:"namespace" env:"NAMESPACE" required:"true" description:"The namespace workspace pods run in"`
	Image          string        `long:"image" env:"IMAGE" required:"true" description:"The workspace-server image every pod runs"`
	DisableTLS     bool          `long:"disable-tls" env:"DISABLE_TLS" description:"Serve and dial the workspace-server in plaintext"`
	Env            []string      `long:"env" env:"ENV" env-delim:"," description:"KEY=VALUE env vars for every workspace-server: its TLS and authentication flags"`
	IdleTimeout    time.Duration `long:"idle-timeout" env:"IDLE_TIMEOUT" default:"30m" description:"How long a running workspace may go unused before the reaper stops it"`
	MaxRunDuration time.Duration `long:"max-run-duration" env:"MAX_RUN_DURATION" default:"12h" description:"How long a pod may run, used or not, before the reaper stops it"`
}

// secretsDialer reaches the SecretService of a running pod. close releases it.
type secretsDialer func(ctx context.Context, pod *corev1.Pod) (client workspaceservicepb.SecretServiceClient, close func() error, err error)

// operationWaiter waits on an operation another service started on the shared scheduler.
type operationWaiter interface {
	WaitOperation(ctx context.Context, request *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error)
}

type runtime struct {
	podConfig *pod.Config
	// pods, dialSecrets and operations are set on start, or by tests.
	pods        *pod.Pods
	dialSecrets secretsDialer
	operations  operationWaiter
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.IdleTimeout <= 0 || opts.MaxRunDuration <= 0 {
		return nil, fmt.Errorf("idle-timeout and max-run-duration must be positive")
	}
	var env []corev1.EnvVar
	for _, keyValue := range opts.Env {
		key, value, ok := strings.Cut(keyValue, "=")
		if !ok {
			return nil, fmt.Errorf("env %q is not KEY=VALUE", keyValue)
		}
		env = append(env, corev1.EnvVar{Name: key, Value: value})
	}
	podConfig := &pod.Config{
		Namespace:  opts.Namespace,
		Image:      opts.Image,
		DisableTLS: opts.DisableTLS,
		Env:        env,
	}
	return &runtime{podConfig: podConfig}, nil
}

// start connects to the cluster and runs the reaper.
func (s *Service) start(ctx context.Context) (func(), error) {
	if s.pods == nil {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("loading in-cluster config: %w", err)
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("creating kubernetes client: %w", err)
		}
		s.pods = pod.NewPods(clientset, s.opts.Namespace)
	}
	if s.dialSecrets == nil {
		s.dialSecrets = s.dialPodSecrets
	}
	if s.operations == nil {
		s.operations = longrunning.NewServer(s.schedulerServiceClient, []string{createSnapshotMethod})
	}
	reaper := routine.New("workspace-reap", s.reap).
		WithTicker(reapInterval).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx)
	return reaper.Close, nil
}

// dialPodSecrets dials a running pod's workspace-server by its IP.
func (s *Service) dialPodSecrets(ctx context.Context, workspacePod *corev1.Pod) (workspaceservicepb.SecretServiceClient, func() error, error) {
	clientOpts, err := s.podConfig.ClientOpts(workspacePod)
	if err != nil {
		return nil, nil, err
	}
	connection, err := grpc.NewConnection(clientOpts, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("creating workspace-server connection: %w", err)
	}
	if err := connection.Connect(ctx); err != nil {
		return nil, nil, fmt.Errorf("connecting to workspace-server: %w", err)
	}
	return workspaceservicepb.NewSecretServiceClient(connection.Get()), connection.Close, nil
}
