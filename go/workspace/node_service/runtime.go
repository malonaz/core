// Package node_service is the workspace agent's share of the filesystems' work on one node: it
// snapshots LOCAL filesystems to object storage, restores and purges them, and is the CSI node
// plugin kubelet calls to mount a filesystem into a workspace pod.
package node_service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/prometheus"
)

const (
	// localDirectory holds a LOCAL filesystem's files, at its resource name.
	localDirectory = "local"
	// sharedDirectory is the network filesystem every node mounts, a directory per SHARED filesystem.
	sharedDirectory = "shared"
	// stateDirectory holds what the node knows of each LOCAL filesystem's copy, beside its files.
	stateDirectory = "state"
	// temporaryDirectory holds snapshots being written and copies being removed.
	temporaryDirectory = "tmp"

	// fullSnapshotTurns is how many incremental snapshots may follow a full one before the next is full.
	fullSnapshotTurns = 20
	// fullSnapshotRatio makes the next snapshot full once the incrementals since the last full
	// exceed 1/fullSnapshotRatio of its size, keeping a restore to ~1.25x one full.
	fullSnapshotRatio = 4
	// maxRecordedRequests is how many snapshot request ids each filesystem remembers, to answer retries.
	maxRecordedRequests = 16
)

type Opts struct {
	Directory     string `long:"directory" env:"DIRECTORY" default:"/var/lib/workspaces" description:"The node's workspace disk: LOCAL filesystems in local/, the SHARED network filesystem mounted at shared/"`
	Bucket        string `long:"bucket" env:"BUCKET" required:"true" description:"The S3 bucket holding LOCAL filesystems' snapshots"`
	NodeName      string `long:"node-name" env:"NODE_NAME" required:"true" description:"The node this agent runs on, which kubelet asks the CSI plugin for"`
	CSISocketPath string `long:"csi-socket-path" env:"CSI_SOCKET_PATH" default:"/csi/csi.sock" description:"The unix socket kubelet calls the CSI plugin on"`
}

type runtime struct {
	directory string
	// objects and mounter are set on start, or by tests.
	objects objects
	mounter mounter
	// filesystemToMutex serializes the work on each filesystem's copy.
	filesystemToMutex sync.Map
}

func newRuntime(opts *Opts) (*runtime, error) {
	directory, err := filepath.Abs(opts.Directory)
	if err != nil {
		return nil, fmt.Errorf("resolving directory: %w", err)
	}
	return &runtime{directory: directory}, nil
}

// start creates the disk's layout, connects to S3 and serves the CSI plugin to kubelet.
func (s *Service) start(ctx context.Context) (func(), error) {
	if err := s.createLayout(); err != nil {
		return nil, err
	}
	if s.objects == nil {
		awsConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading aws config: %w", err)
		}
		s.objects = &s3Objects{client: s3.NewFromConfig(awsConfig), bucket: s.opts.Bucket}
	}
	if s.mounter == nil {
		s.mounter = bindMounter{}
	}

	// Kubelet dials the socket in plaintext, as the node's root.
	serverOpts := &grpc.ServerOpts{
		Health:              &health.GRPCOpts{Interval: 5 * time.Second, Timeout: 30 * time.Second, Startup: 30 * time.Second},
		DisableTLS:          true,
		GracefulStopTimeout: 10,
		SocketPath:          s.opts.CSISocketPath,
	}
	register := func(server *grpc.Server) {
		csi.RegisterIdentityServer(server.Raw, &identityServer{})
		csi.RegisterNodeServer(server.Raw, &nodeServer{service: s})
	}
	// Metrics on: kubelet's calls show in the binary's gRPC server metrics as the `csi.v1` services.
	csiServer, err := grpc.NewServer(serverOpts, nil, &prometheus.Opts{}, "csi", grpc.ServerOptions{}, register)
	if err != nil {
		return nil, fmt.Errorf("creating csi server: %w", err)
	}
	csiServer.WithLogger(s.log)
	if err := csiServer.Listen(ctx); err != nil {
		return nil, fmt.Errorf("listening on csi socket: %w", err)
	}
	go func() {
		if err := csiServer.Serve(ctx); err != nil {
			s.log.ErrorContext(ctx, "serving csi", "error", err)
		}
	}()
	return func() {
		if err := csiServer.GracefulStop(); err != nil {
			s.log.Error("stopping csi server", "error", err)
		}
	}, nil
}

// createLayout creates the disk's directories.
func (r *runtime) createLayout() error {
	for _, name := range []string{localDirectory, sharedDirectory, stateDirectory, temporaryDirectory} {
		if err := os.MkdirAll(filepath.Join(r.directory, name), 0o755); err != nil {
			return fmt.Errorf("creating %s directory: %w", name, err)
		}
	}
	return nil
}

// lockFilesystem serializes work on a filesystem's copy and returns its unlock.
func (r *runtime) lockFilesystem(filesystem string) func() {
	value, _ := r.filesystemToMutex.LoadOrStore(filesystem, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}
