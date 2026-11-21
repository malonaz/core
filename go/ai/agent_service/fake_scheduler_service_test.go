package agent_service

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
)

// The scheduler's own list parsers: the fake rejects any filter the real service would.
var (
	fakeListSchedulesRequestParser = aip.MustNewListRequestParser[*schedulerservicepb.ListSchedulesRequest, *schedulerpb.Schedule](aip.WithFilteringOpts(aip.WithFQN()))
	fakeListJobsRequestParser      = aip.MustNewListRequestParser[*schedulerservicepb.ListJobsRequest, *schedulerpb.Job](aip.WithFilteringOpts(aip.WithFQN()))
)

// signedSessionHeader carries a caller's signed session on outgoing calls.
const signedSessionHeader = "x-signed-session-bin"

// fakeSchedulerService is an in-memory SchedulerService client of schedules and jobs. It interprets no filter: it
// matches the AgentService's own builders, recomputed per item for the agent it is labeled with. It admits the
// service's identity alone, as the real one's interceptors do. Methods not implemented panic.
type fakeSchedulerService struct {
	schedulerservicepb.SchedulerServiceClient

	mutex               sync.Mutex
	schedules           []*schedulerpb.Schedule
	jobs                []*schedulerpb.Job
	requestIDToSchedule map[string]*schedulerpb.Schedule
	requestIDToJob      map[string]*schedulerpb.Job
	etagCounter         int
	// cancelJobErr, if set, fails every CancelJob.
	cancelJobErr error
}

func newFakeSchedulerService() *fakeSchedulerService {
	return &fakeSchedulerService{
		requestIDToSchedule: map[string]*schedulerpb.Schedule{},
		requestIDToJob:      map[string]*schedulerpb.Job{},
	}
}

// authenticate refuses a call not made as the service, or carrying a caller's signed session besides: the real
// scheduler's interceptors would find two sessions.
func authenticate(ctx context.Context) error {
	outgoingMetadata, _ := metadata.FromOutgoingContext(ctx)
	if len(outgoingMetadata.Get(testServiceAccountHeader)) != 1 {
		return status.Errorf(codes.Unauthenticated, "not called as the service").Err()
	}
	if len(outgoingMetadata.Get(signedSessionHeader)) > 0 {
		return status.Errorf(codes.Internal, "unexpected signed session in local context").Err()
	}
	return nil
}

// asTestService returns a context calling the fake scheduler as the service.
func asTestService() context.Context {
	return withTestServiceAccount(context.Background())
}

// failCancelJobs makes every CancelJob fail with err, or succeed again with nil.
func (c *fakeSchedulerService) failCancelJobs(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cancelJobErr = err
}

// nextEtag returns a fresh etag.
func (c *fakeSchedulerService) nextEtag() string {
	c.etagCounter++
	return strconv.Itoa(c.etagCounter)
}

// CreateSchedule creates an enabled schedule, its next run a minute away: a replayed request id returns it.
func (c *fakeSchedulerService) CreateSchedule(ctx context.Context, request *schedulerservicepb.CreateScheduleRequest, _ ...grpc.CallOption) (*schedulerpb.Schedule, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if schedule, ok := c.requestIDToSchedule[request.RequestId]; ok && request.RequestId != "" {
		return proto.CloneOf(schedule), nil
	}
	scheduleRn, err := schedulerpb.NewScheduleRn(request.Parent, aip.NewSystemGeneratedBase32ResourceID())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	schedule := proto.CloneOf(request.Schedule)
	schedule.Name = scheduleRn.String()
	schedule.State = schedulerpb.ScheduleState_SCHEDULE_STATE_ENABLED
	schedule.CreateTime = timestamppb.Now()
	schedule.NextScheduleTime = timestamppb.New(time.Now().Add(time.Minute))
	schedule.Etag = c.nextEtag()
	c.schedules = append(c.schedules, schedule)
	if request.RequestId != "" {
		c.requestIDToSchedule[request.RequestId] = schedule
	}
	return proto.CloneOf(schedule), nil
}

func (c *fakeSchedulerService) GetSchedule(ctx context.Context, request *schedulerservicepb.GetScheduleRequest, _ ...grpc.CallOption) (*schedulerpb.Schedule, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	index := slices.IndexFunc(c.schedules, func(schedule *schedulerpb.Schedule) bool { return schedule.Name == request.Name })
	if index < 0 {
		return nil, status.Errorf(codes.NotFound, "schedule %s does not exist", request.Name).Err()
	}
	return proto.CloneOf(c.schedules[index]), nil
}

// ListSchedules lists a parent's schedules in creation order, in one page.
func (c *fakeSchedulerService) ListSchedules(ctx context.Context, request *schedulerservicepb.ListSchedulesRequest, _ ...grpc.CallOption) (*schedulerservicepb.ListSchedulesResponse, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	if _, err := fakeListSchedulesRequestParser.Parse(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	listSchedulesResponse := &schedulerservicepb.ListSchedulesResponse{}
	for _, schedule := range c.schedules {
		if parent, _ := scheduleParent(schedule.Name); parent != request.Parent {
			continue
		}
		if matchesScheduleFilter(request.Parent, schedule, request.Filter) {
			listSchedulesResponse.Schedules = append(listSchedulesResponse.Schedules, proto.CloneOf(schedule))
		}
	}
	return listSchedulesResponse, nil
}

// DeleteSchedule deletes a schedule, checking its etag if set.
func (c *fakeSchedulerService) DeleteSchedule(ctx context.Context, request *schedulerservicepb.DeleteScheduleRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	index := slices.IndexFunc(c.schedules, func(schedule *schedulerpb.Schedule) bool { return schedule.Name == request.Name })
	switch {
	case index < 0 && request.AllowMissing:
		return &emptypb.Empty{}, nil
	case index < 0:
		return nil, status.Errorf(codes.NotFound, "schedule %s does not exist", request.Name).Err()
	case request.Etag != "" && request.Etag != c.schedules[index].Etag:
		return nil, status.Errorf(codes.Aborted, "etag changed").Err()
	}
	c.schedules = slices.Delete(c.schedules, index, index+1)
	return &emptypb.Empty{}, nil
}

// CreateJob creates a pending job: a replayed request id returns it.
func (c *fakeSchedulerService) CreateJob(ctx context.Context, request *schedulerservicepb.CreateJobRequest, _ ...grpc.CallOption) (*schedulerpb.Job, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if job, ok := c.requestIDToJob[request.RequestId]; ok && request.RequestId != "" {
		return proto.CloneOf(job), nil
	}
	job := proto.CloneOf(request.Job)
	if err := c.addJob(request.Parent, job); err != nil {
		return nil, err
	}
	if request.RequestId != "" {
		c.requestIDToJob[request.RequestId] = job
	}
	return proto.CloneOf(job), nil
}

// addJob names a job under a parent and stores it pending.
func (c *fakeSchedulerService) addJob(parent string, job *schedulerpb.Job) error {
	jobRn, err := schedulerpb.NewJobRn(parent, aip.NewSystemGeneratedBase32ResourceID())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	job.Name = jobRn.String()
	job.State = schedulerpb.JobState_JOB_STATE_PENDING
	job.CreateTime = timestamppb.Now()
	if job.ScheduleTime == nil {
		job.ScheduleTime = job.CreateTime
	}
	job.Etag = c.nextEtag()
	c.jobs = append(c.jobs, job)
	return nil
}

func (c *fakeSchedulerService) GetJob(ctx context.Context, request *schedulerservicepb.GetJobRequest, _ ...grpc.CallOption) (*schedulerpb.Job, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	job := c.findJob(request.Name)
	if job == nil {
		return nil, status.Errorf(codes.NotFound, "job %s does not exist", request.Name).Err()
	}
	return proto.CloneOf(job), nil
}

func (c *fakeSchedulerService) findJob(name string) *schedulerpb.Job {
	index := slices.IndexFunc(c.jobs, func(job *schedulerpb.Job) bool { return job.Name == name })
	if index < 0 {
		return nil
	}
	return c.jobs[index]
}

// ListJobs lists a parent's jobs by schedule time, in one page.
func (c *fakeSchedulerService) ListJobs(ctx context.Context, request *schedulerservicepb.ListJobsRequest, _ ...grpc.CallOption) (*schedulerservicepb.ListJobsResponse, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	if _, err := fakeListJobsRequestParser.Parse(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	listJobsResponse := &schedulerservicepb.ListJobsResponse{}
	for _, job := range c.jobs {
		if parent, _ := jobParent(job.Name); parent != request.Parent {
			continue
		}
		if matchesJobFilter(request.Parent, job, request.Filter) {
			listJobsResponse.Jobs = append(listJobsResponse.Jobs, proto.CloneOf(job))
		}
	}
	slices.SortStableFunc(listJobsResponse.Jobs, func(a, b *schedulerpb.Job) int {
		return a.ScheduleTime.AsTime().Compare(b.ScheduleTime.AsTime())
	})
	return listJobsResponse, nil
}

// CancelJob cancels a pending job.
func (c *fakeSchedulerService) CancelJob(ctx context.Context, request *schedulerservicepb.CancelJobRequest, _ ...grpc.CallOption) (*schedulerpb.Job, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.cancelJobErr != nil {
		return nil, c.cancelJobErr
	}
	job := c.findJob(request.Name)
	if job == nil {
		return nil, status.Errorf(codes.NotFound, "job %s does not exist", request.Name).Err()
	}
	if job.State != schedulerpb.JobState_JOB_STATE_PENDING {
		return nil, status.Errorf(codes.FailedPrecondition, "job %s is %s", request.Name, job.State).Err()
	}
	job.State = schedulerpb.JobState_JOB_STATE_CANCELLED
	job.Etag = c.nextEtag()
	return proto.CloneOf(job), nil
}

// scheduleParent and jobParent return the parent of a resource name.
func scheduleParent(name string) (string, error) {
	scheduleRn, err := schedulerpb.ParseScheduleRn(name)
	if err != nil {
		return "", err
	}
	return scheduleRn.Parent(), nil
}

func jobParent(name string) (string, error) {
	jobRn, err := schedulerpb.ParseJobRn(name)
	if err != nil {
		return "", err
	}
	return jobRn.Parent(), nil
}

// labeledAgentRn returns the agent a schedule or job under a user is labeled with, if any.
func labeledAgentRn(parent string, labels map[string]string) (*agentpb.AgentRn, bool) {
	agentID, ok := labels[agentpb.Labels.Agent.GetKey()]
	if !ok {
		return nil, false
	}
	userRn, err := agentpb.ParseUserRn(parent)
	if err != nil {
		return nil, false
	}
	return userRn.AgentRn(agentID), true
}

// matchesScheduleFilter matches a schedule against the filter the AgentService sends: any other matches nothing.
func matchesScheduleFilter(parent string, schedule *schedulerpb.Schedule, filter string) bool {
	agentRn, ok := labeledAgentRn(parent, schedule.Labels)
	return filter == "" || ok && filter == agentScheduleFilter(agentRn)
}

// matchesJobFilter matches a job against the filters the AgentService sends: any other matches nothing.
func matchesJobFilter(parent string, job *schedulerpb.Job, filter string) bool {
	if filter == "" {
		return true
	}
	agentRn, ok := labeledAgentRn(parent, job.Labels)
	if !ok {
		return false
	}
	pending := job.State == schedulerpb.JobState_JOB_STATE_PENDING
	switch filter {
	case agentScheduleFilter(agentRn):
		return true
	case pendingRunFilter(agentRn):
		return pending
	case pendingOneOffFilter(agentRn):
		return pending && job.Schedule == ""
	}
	return false
}

// schedulesOf returns the schedules labeled with an agent, under its user.
func (c *fakeSchedulerService) schedulesOf(t *testing.T, agent string) []*schedulerpb.Schedule {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	listSchedulesRequest := &schedulerservicepb.ListSchedulesRequest{Parent: agentRn.UserRn().String(), Filter: agentScheduleFilter(agentRn)}
	listSchedulesResponse, err := c.ListSchedules(asTestService(), listSchedulesRequest)
	require.NoError(t, err)
	return listSchedulesResponse.Schedules
}

// jobsOf returns the jobs labeled with an agent, under its user, whatever their state.
func (c *fakeSchedulerService) jobsOf(t *testing.T, agent string) []*schedulerpb.Job {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	listJobsRequest := &schedulerservicepb.ListJobsRequest{Parent: agentRn.UserRn().String(), Filter: agentScheduleFilter(agentRn)}
	listJobsResponse, err := c.ListJobs(asTestService(), listJobsRequest)
	require.NoError(t, err)
	return listJobsResponse.Jobs
}

// tick materializes a tick of a schedule as the scheduler does: a pending job under its parent, carrying its labels
// and payload, naming it.
func (c *fakeSchedulerService) tick(t *testing.T, scheduleName string) *schedulerpb.Job {
	t.Helper()
	getScheduleRequest := &schedulerservicepb.GetScheduleRequest{Name: scheduleName}
	schedule, err := c.GetSchedule(asTestService(), getScheduleRequest)
	require.NoError(t, err)
	parent, err := scheduleParent(scheduleName)
	require.NoError(t, err)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	job := &schedulerpb.Job{Schedule: scheduleName, Labels: schedule.Labels, Payload: schedule.Payload, ScheduleTime: schedule.NextScheduleTime}
	require.NoError(t, c.addJob(parent, job))
	return proto.CloneOf(job)
}

// run runs a job's payload as the scheduler does: WakeAgent with the job's name in the incoming metadata.
func (c *fakeSchedulerService) run(t *testing.T, service *testReplica, payload *anypb.Any, jobName string) (*agentpb.Agent, error) {
	t.Helper()
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{}
	require.NoError(t, payload.UnmarshalTo(wakeAgentRequest))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(scheduler.JobMetadataKey, jobName))
	return service.WakeAgent(ctx, wakeAgentRequest)
}
