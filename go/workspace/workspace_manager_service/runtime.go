// Package workspace_manager_service runs workspaces: it creates and deletes their pods, attaches
// the filesystems they mount, snapshots them, and reconciles their pods with their state.
package workspace_manager_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

// reconcileInterval is how often the reconciler compares pods to their workspaces.
const reconcileInterval = time.Minute

type Opts struct {
	Namespace      string        `long:"namespace" env:"NAMESPACE" required:"true" description:"The namespace workspace pods run in"`
	Image          string        `long:"image" env:"IMAGE" required:"true" description:"The workspace-server image every pod runs"`
	DisableTLS     bool          `long:"disable-tls" env:"DISABLE_TLS" description:"Serve and dial the workspace-server in plaintext"`
	CPURequest     string        `long:"cpu-request" env:"CPU_REQUEST" default:"250m" description:"CPU each workspace requests"`
	CPULimit       string        `long:"cpu-limit" env:"CPU_LIMIT" default:"2" description:"CPU each workspace may use"`
	MemoryRequest  string        `long:"memory-request" env:"MEMORY_REQUEST" default:"512Mi" description:"Memory each workspace requests"`
	MemoryLimit    string        `long:"memory-limit" env:"MEMORY_LIMIT" default:"2Gi" description:"Memory each workspace may use"`
	Env            []string      `long:"env" env:"ENV" env-delim:"," description:"KEY=VALUE env vars for every workspace-server: its TLS and authentication flags"`
	IdleTimeout    time.Duration `long:"idle-timeout" env:"IDLE_TIMEOUT" default:"30m" description:"How long a running workspace may go unused before the reconciler stops it"`
	MaxRunDuration time.Duration `long:"max-run-duration" env:"MAX_RUN_DURATION" default:"12h" description:"How long a pod may run, used or not, before the reconciler stops it"`
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
	resources := corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}}
	for _, quantity := range []struct {
		list  corev1.ResourceList
		name  corev1.ResourceName
		value string
	}{
		{resources.Requests, corev1.ResourceCPU, opts.CPURequest},
		{resources.Limits, corev1.ResourceCPU, opts.CPULimit},
		{resources.Requests, corev1.ResourceMemory, opts.MemoryRequest},
		{resources.Limits, corev1.ResourceMemory, opts.MemoryLimit},
	} {
		parsed, err := resource.ParseQuantity(quantity.value)
		if err != nil {
			return nil, fmt.Errorf("parsing %s %q: %w", quantity.name, quantity.value, err)
		}
		quantity.list[quantity.name] = parsed
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
		Resources:  resources,
		Env:        env,
	}
	return &runtime{podConfig: podConfig}, nil
}

// start connects to the cluster and runs the reconciler.
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
	reconciler := routine.New("workspace-reconcile", s.reconcile).
		WithTicker(reconcileInterval).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx)
	return reconciler.Close, nil
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
