package scheduler_service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbreflection"
	"github.com/malonaz/core/go/routine"
)

type Opts struct {
	FileDescriptorSets []string      `long:"file-descriptor-set" env:"FILE_DESCRIPTOR_SET" env-delim:"," description:"Path to a file descriptor set holding the methods queue handlers may route to; repeatable. A ':services' suffix, as accepted by ai-engine, is ignored"`
	MaxParallelJobs    int           `long:"max-parallel-jobs" env:"MAX_PARALLEL_JOBS" default:"50" description:"Jobs this instance processes concurrently"`
	PollInterval       time.Duration `long:"poll-interval" env:"POLL_INTERVAL" default:"1s" description:"Interval between claim scans while idle"`
	LeaseDuration      time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease held on a running job, renewed while its handler call is in flight; a lapsed lease returns the job to PENDING"`
	Retention          time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever"`
	SweepInterval      time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"1h" description:"Interval between retention sweeps"`
	WorkerID           string        `long:"worker-id" env:"WORKER_ID" description:"Identifies this instance on the jobs it runs; defaults to hostname:pid"`
}

type runtime struct {
	// The only source of method and message type information: what handlers may route to.
	files   *protoregistry.Files
	targets *targetConnections

	// Workers in flight on this instance, so CancelJob can cut a local call short.
	inflight *inflight
	// Bounds concurrency; the claim loop only asks for as many jobs as there are free slots.
	slots chan struct{}
	// Woken by the claim loop after a full batch, and by CreateJob, so work starts without waiting for the ticker.
	claimSignal chan struct{}
	workers     sync.WaitGroup
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.MaxParallelJobs < 1 {
		return nil, fmt.Errorf("max-parallel-jobs must be at least 1")
	}
	if opts.LeaseDuration <= 0 || opts.PollInterval <= 0 || opts.SweepInterval <= 0 {
		return nil, fmt.Errorf("lease-duration, poll-interval and sweep-interval must be positive")
	}
	if opts.WorkerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolving hostname: %w", err)
		}
		opts.WorkerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	files, err := loadFileDescriptorSets(opts.FileDescriptorSets)
	if err != nil {
		return nil, fmt.Errorf("loading file descriptor sets: %w", err)
	}
	if err := registerGlobalTypes(files); err != nil {
		return nil, fmt.Errorf("registering descriptor set types: %w", err)
	}
	return &runtime{
		files:       files,
		targets:     newTargetConnections(),
		inflight:    newInflight(),
		slots:       make(chan struct{}, opts.MaxParallelJobs),
		claimSignal: make(chan struct{}, 1),
	}, nil
}

// loadFileDescriptorSets reads and merges the descriptor sets into one registry.
func loadFileDescriptorSets(configs []string) (*protoregistry.Files, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("at least one --file-descriptor-set is required")
	}
	aggregate := &descriptorpb.FileDescriptorSet{}
	fileNameSet := map[string]struct{}{}
	for _, config := range configs {
		// The ai-engine form 'path:service,...' is accepted; the whole set is taken regardless.
		path, _, _ := strings.Cut(config, ":")
		bytes, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %q: %w", path, err)
		}
		fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
		if err := pbutil.Unmarshal(bytes, fileDescriptorSet); err != nil {
			return nil, fmt.Errorf("parsing %q: %w", path, err)
		}
		for _, file := range fileDescriptorSet.GetFile() {
			if _, ok := fileNameSet[file.GetName()]; ok {
				continue
			}
			fileNameSet[file.GetName()] = struct{}{}
			aggregate.File = append(aggregate.File, file)
		}
	}
	return protodesc.NewFiles(aggregate)
}

// registerGlobalTypes makes the descriptor set's messages resolvable through
// the global type registry, which protojson consults: without it, payloads and
// responses of types this binary does not link would render as opaque Any.
func registerGlobalTypes(files *protoregistry.Files) error {
	types, err := pbreflection.NewTypesFromFiles(files)
	if err != nil {
		return err
	}
	var registrationErr error
	types.RangeMessages(func(messageType protoreflect.MessageType) bool {
		if _, err := protoregistry.GlobalTypes.FindMessageByName(messageType.Descriptor().FullName()); err == nil {
			return true
		}
		registrationErr = protoregistry.GlobalTypes.RegisterMessage(messageType)
		return registrationErr == nil
	})
	return registrationErr
}

// resolveMethod returns the descriptor of a "/package.Service/Method" gRPC
// method from the descriptor set.
func (r *runtime) resolveMethod(method string) (protoreflect.MethodDescriptor, error) {
	fullName := protoreflect.FullName(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "."))
	descriptor, err := r.files.FindDescriptorByName(fullName)
	if err != nil {
		return nil, fmt.Errorf("method %q is not in the descriptor set", method)
	}
	methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a method", method)
	}
	return methodDescriptor, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	// Workers outlive the routines that spawn them: they are cancelled and drained last.
	workerCtx, cancelWorkers := context.WithCancel(ctx)

	routines := []*routine.Routine{
		routine.New("scheduler-claim", func(ctx context.Context) error { return s.claim(ctx, workerCtx) }).
			WithTicker(s.opts.PollInterval).WithSignal(s.claimSignal).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx),
		routine.New("scheduler-reap", s.reap).
			WithTicker(s.opts.LeaseDuration / 2).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx),
	}
	if s.opts.Retention > 0 {
		routines = append(routines, routine.New("scheduler-sweep", s.sweep).
			WithTicker(s.opts.SweepInterval).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx))
	}

	return func() {
		for _, backgroundRoutine := range routines {
			backgroundRoutine.Close()
		}
		cancelWorkers()
		s.workers.Wait()
		s.targets.close()
	}, nil
}

// inflight tracks the cancel function of every job this instance is processing.
type inflight struct {
	mutex         sync.Mutex
	jobIDToCancel map[string]context.CancelFunc
}

func newInflight() *inflight {
	return &inflight{jobIDToCancel: map[string]context.CancelFunc{}}
}

func (i *inflight) add(jobID string, cancel context.CancelFunc) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.jobIDToCancel[jobID] = cancel
}

func (i *inflight) remove(jobID string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	delete(i.jobIDToCancel, jobID)
}

// cancel cuts the job's handler call short if it runs on this instance.
func (i *inflight) cancel(jobID string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if cancel, ok := i.jobIDToCancel[jobID]; ok {
		cancel()
	}
}
