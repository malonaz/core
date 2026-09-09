package scheduler_service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/routine"
)

type Opts struct {
	Configuration     string        `long:"configuration" env:"CONFIGURATION" description:"Path to the jsonnet configuration"`
	IgnoreJobTypeURLs []string      `long:"ignore-job" env:"IGNORE_JOB" env-delim:"," description:"Job type URLs this instance leaves unclaimed"`
	MaxParallelJobs   int           `long:"max-parallel-jobs" env:"MAX_PARALLEL_JOBS" default:"50" description:"Jobs this instance processes concurrently"`
	PollInterval      time.Duration `long:"poll-interval" env:"POLL_INTERVAL" default:"1s" description:"Interval between claim scans while idle"`
	LeaseDuration     time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease held on a running job, renewed while its processor call is in flight; a lapsed lease returns the job to PENDING"`
	Retention         time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever"`
	SweepInterval     time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"1h" description:"Interval between retention sweeps"`
}

var defaultRetryBackoff = &pb.RetryBackoff{
	Initial:    durationpb.New(10 * time.Second),
	Max:        durationpb.New(10 * time.Minute),
	Multiplier: 2,
}

type runtime struct {
	configuration               *pb.Configuration
	ignoredJobTypes             []string
	jobTypeToConfiguration      map[string]*pb.JobTypeConfiguration
	processorIDToProcessor      map[string]*pb.Processor
	processorIDToGRPCConnection map[string]*grpc.Connection

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

	bytes, err := jsonnet.EvaluateFile(opts.Configuration)
	if err != nil {
		return nil, fmt.Errorf("evaluating configuration: %w", err)
	}
	configuration := &pb.Configuration{}
	if err := pbutil.JSONUnmarshalStrict(bytes, configuration); err != nil {
		return nil, fmt.Errorf("parsing configuration: %w", err)
	}
	if err := protovalidate.Validate(configuration); err != nil {
		return nil, fmt.Errorf("validating configuration: %w", err)
	}

	processorIDToProcessor := map[string]*pb.Processor{}
	for _, processor := range configuration.GetProcessors() {
		if _, ok := processorIDToProcessor[processor.GetId()]; ok {
			return nil, fmt.Errorf("duplicate processor %q", processor.GetId())
		}
		processorIDToProcessor[processor.GetId()] = processor
	}
	jobTypeToConfiguration := map[string]*pb.JobTypeConfiguration{}
	for _, jobTypeConfiguration := range configuration.GetJobTypeConfigurations() {
		jobType := jobTypeConfiguration.GetJobTypeUrl()
		if _, ok := jobTypeToConfiguration[jobType]; ok {
			return nil, fmt.Errorf("duplicate job type %q", jobType)
		}
		if _, ok := processorIDToProcessor[jobTypeConfiguration.GetProcessorId()]; !ok {
			return nil, fmt.Errorf("job type %q targets unknown processor %q", jobType, jobTypeConfiguration.GetProcessorId())
		}
		if jobTypeConfiguration.RetryBackoff == nil {
			jobTypeConfiguration.RetryBackoff = defaultRetryBackoff
		}
		jobTypeToConfiguration[jobType] = jobTypeConfiguration
	}

	// An empty array (never NULL) keeps the claim query's `job_type = ANY($n)` well-defined.
	ignoredJobTypes := append([]string{}, opts.IgnoreJobTypeURLs...)

	return &runtime{
		configuration:               configuration,
		ignoredJobTypes:             ignoredJobTypes,
		jobTypeToConfiguration:      jobTypeToConfiguration,
		processorIDToProcessor:      processorIDToProcessor,
		processorIDToGRPCConnection: map[string]*grpc.Connection{},
		inflight:                    newInflight(),
		slots:                       make(chan struct{}, opts.MaxParallelJobs),
		claimSignal:                 make(chan struct{}, 1),
	}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	for processorID, processor := range s.processorIDToProcessor {
		connection, err := s.connectProcessor(ctx, processor)
		if err != nil {
			return nil, fmt.Errorf("connecting to processor %q: %w", processorID, err)
		}
		s.processorIDToGRPCConnection[processorID] = connection
	}

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
		for _, connection := range s.processorIDToGRPCConnection {
			connection.Close()
		}
	}, nil
}

func (s *Service) connectProcessor(ctx context.Context, processor *pb.Processor) (*grpc.Connection, error) {
	opts, err := grpc.ParseOpts(processor.GetUrl())
	if err != nil {
		return nil, fmt.Errorf("parsing url %q: %w", processor.GetUrl(), err)
	}
	connection, err := grpc.NewConnection(opts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	if err := connection.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	return connection, nil
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

// cancel cuts the job's processor call short if it runs on this instance.
func (i *inflight) cancel(jobID string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if cancel, ok := i.jobIDToCancel[jobID]; ok {
		cancel()
	}
}
