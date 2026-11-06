package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// wakeNamespace derives the request ids of a run from its job: a run replayed or retried creates nothing twice.
var wakeNamespace = uuid.MustParse("7f550727-4cff-4528-b005-252e0434d446")

// WakeAgent runs one of an agent's schedules: it queues the run's message on the agent, or launches the run's
// sub-agent with its task.
func (s *Service) WakeAgent(ctx context.Context, request *agentservicepb.WakeAgentRequest) (*agentpb.Agent, error) {
	job, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "WakeAgent runs only as a scheduler job").Err()
	}
	requestID := func(part string) string { return uuid.NewV5(wakeNamespace, job+"/"+part).String() }
	// A run acts as the service, as turns do: the agent's tools still act as its principal.
	ctx = s.asService(ctx)

	switch wake := request.Wake.(type) {
	case *agentservicepb.WakeAgentRequest_Message:
		// Sent by the agent itself, which scheduled it.
		message := ai.NewUserMessage(ai.NewTextBlock(wake.Message))
		return redacted(s.messageAgent(ctx, request.Name, message, request.Name, requestID("message")))
	case *agentservicepb.WakeAgentRequest_SubAgent:
		agent, err := s.getAgent(ctx, request.Name)
		if err != nil {
			return nil, err
		}
		// Validated again at each run: the payload is only a request, whoever created it.
		subAgent, err := s.validateSubAgent(agent, wake.SubAgent)
		if err != nil {
			return nil, err
		}
		return s.launchSubAgent(ctx, agent, subAgent, wake.SubAgent.GetTask(), requestID("agent"), requestID("task"))
	default:
		return nil, status.Errorf(codes.InvalidArgument, "wake is required").Err()
	}
}

// launchSubAgent creates a validated sub-agent of an agent and sends it its task, returning it.
func (s *Service) launchSubAgent(ctx context.Context, agent, subAgent *agentpb.Agent, task, agentRequestID, taskRequestID string) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent:    agentRn.UserRn().String(),
		Agent:     subAgent,
		RequestId: agentRequestID,
	}
	if err := protovalidate.Validate(createAgentRequest); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	createdAgent, err := s.CreateAgent(ctx, createAgentRequest)
	if err != nil {
		return nil, err
	}
	message := ai.NewUserMessage(ai.NewTextBlock(task))
	return redacted(s.messageAgent(ctx, createdAgent.GetName(), message, agent.GetName(), taskRequestID))
}

// agentScheduleFilter selects the schedules and jobs waking an agent, under its user.
func agentScheduleFilter(agentRn *agentpb.AgentRn) string {
	return fmt.Sprintf("labels.%q = %q", agentpb.Labels.Agent.GetKey(), agentRn.Agent)
}

// pendingRunFilter selects an agent's pending runs: its one-offs, and its schedules' ticks, which carry their labels.
func pendingRunFilter(agentRn *agentpb.AgentRn) string {
	return agentScheduleFilter(agentRn) + " AND state = JOB_STATE_PENDING"
}

// pendingOneOffFilter selects an agent's pending one-off runs.
func pendingOneOffFilter(agentRn *agentpb.AgentRn) string {
	return pendingRunFilter(agentRn) + " AND NOT schedule:*"
}

// cancelSchedules deletes an agent's schedules and cancels its pending runs, one-offs and ticks alike.
func (s *Service) cancelSchedules(ctx context.Context, agentRn *agentpb.AgentRn) error {
	parent := agentRn.UserRn().String()
	listSchedulesRequest := &schedulerservicepb.ListSchedulesRequest{Parent: parent, Filter: agentScheduleFilter(agentRn)}
	schedules, err := aip.Paginate[*schedulerpb.Schedule](ctx, listSchedulesRequest, s.schedulerServiceClient.ListSchedules)
	if err != nil {
		return status.FromError(err, "listing schedules").Err()
	}
	for _, schedule := range schedules {
		deleteScheduleRequest := &schedulerservicepb.DeleteScheduleRequest{Name: schedule.GetName(), AllowMissing: true}
		if _, err := s.schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest); err != nil {
			return status.FromError(err, "deleting schedule %s", schedule.GetName()).Err()
		}
	}
	listJobsRequest := &schedulerservicepb.ListJobsRequest{Parent: parent, Filter: pendingRunFilter(agentRn)}
	jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, s.schedulerServiceClient.ListJobs)
	if err != nil {
		return status.FromError(err, "listing jobs").Err()
	}
	for _, job := range jobs {
		cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: job.GetName()}
		// A run started since fails on the deleted agent by itself.
		if _, err := s.schedulerServiceClient.CancelJob(ctx, cancelJobRequest); err != nil && !status.HasCode(err, codes.FailedPrecondition, codes.NotFound) {
			return status.FromError(err, "cancelling job %s", job.GetName()).Err()
		}
	}
	return nil
}

// ================================= schedule =================================

const scheduleToolDescription = "Schedule yourself to be woken later: recurring with a `cron` and the user's `time_zone` " +
	"(e.g. `0 9 * * *` in `Europe/Paris` for 9am every day, their 9am), or once at a `time` or after a `delay`. " +
	"Each run either wakes you with a `message`, continuing this conversation with everything you know: for follow-ups " +
	"that need your context; or launches a `sub_agent`, as create_agents does, with its task: for self-contained " +
	"recurring work, as each run starts from a fresh context and only its report reaches you. Write the message or task " +
	"for your future self: what to do, and for whom. Returns the schedule's name, for cancel_schedule. " +
	"The runs arrive by themselves: end your turn after scheduling."

func createSchedule(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments *agentservicepb.ScheduleToolArguments) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{Name: agent.GetName()}
	wake := agentpb.Labels.Wake.Self
	switch argumentsWake := arguments.Wake.(type) {
	case *agentservicepb.ScheduleToolArguments_Message:
		wakeAgentRequest.Wake = &agentservicepb.WakeAgentRequest_Message{Message: argumentsWake.Message}
	case *agentservicepb.ScheduleToolArguments_SubAgent:
		// Checked now, so a launch that would fail fails here rather than at every run.
		if _, err := s.validateSubAgent(agent, argumentsWake.SubAgent); err != nil {
			return nil, err
		}
		wakeAgentRequest.Wake = &agentservicepb.WakeAgentRequest_SubAgent{SubAgent: argumentsWake.SubAgent}
		wake = agentpb.Labels.Wake.SubAgent
	}
	labels := map[string]string{agentpb.Labels.Agent.GetKey(): agentRn.Agent, agentpb.Labels.Wake.GetKey(): wake}
	parent := agentRn.UserRn().String()
	requestID := builtinRequestID(agent, toolCall)

	if cron := arguments.GetCron(); cron != "" {
		payload, err := anypb.New(wakeAgentRequest)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "packing payload: %v", err).Err()
		}
		createScheduleRequest := &schedulerservicepb.CreateScheduleRequest{
			Parent:    parent,
			Schedule:  &schedulerpb.Schedule{Payload: payload, Cron: cron, TimeZone: arguments.GetTimeZone(), Labels: labels},
			RequestId: requestID,
		}
		schedule, err := s.schedulerServiceClient.CreateSchedule(ctx, createScheduleRequest)
		if err != nil {
			return nil, err
		}
		content := fmt.Sprintf("Scheduled %s: recurring, next run at %s. End your turn now: each run arrives by itself.",
			schedule.GetName(), schedule.GetNextScheduleTime().AsTime().Format(time.RFC3339))
		return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
	}

	scheduleTime := arguments.GetTime().AsTime()
	if arguments.GetDelay() != nil {
		scheduleTime = s.now().Add(arguments.GetDelay().AsDuration())
	}
	if !scheduleTime.After(s.now()) {
		return nil, status.Errorf(codes.InvalidArgument, "time %s is not in the future", scheduleTime.Format(time.RFC3339)).Err()
	}
	job, err := scheduler.CreateJob(ctx, s.schedulerServiceClient, parent, wakeAgentRequest,
		scheduler.WithScheduleTime(scheduleTime), scheduler.WithLabels(labels), scheduler.WithRequestID(requestID))
	if err != nil {
		return nil, err
	}
	content := fmt.Sprintf("Scheduled %s: once, at %s. End your turn now: the run arrives by itself.",
		job.GetName(), job.GetScheduleTime().AsTime().Format(time.RFC3339))
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

// validateSubAgent validates the launch of a sub-agent by an agent, as create_agents does.
func (s *Service) validateSubAgent(agent *agentpb.Agent, arguments *agentservicepb.CreateAgentsToolArguments_SubAgent) (*agentpb.Agent, error) {
	if err := mayLaunchSubAgents(agent); err != nil {
		return nil, err
	}
	subAgent, err := newSubAgent(agent, arguments)
	if err == nil {
		err = s.validateTools(subAgent.GetSpec())
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sub_agent: %v", err).Err()
	}
	return subAgent, nil
}

// ============================== list_schedules ==============================

const listSchedulesToolDescription = "List your schedules: the recurring ones and the one-off runs still to come, " +
	"each with its name, when it runs and what it does."

func listSchedules(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, _ *agentservicepb.ListSchedulesToolArguments) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	parent := agentRn.UserRn().String()
	listSchedulesRequest := &schedulerservicepb.ListSchedulesRequest{Parent: parent, Filter: agentScheduleFilter(agentRn), OrderBy: "create_time"}
	schedules, err := aip.Paginate[*schedulerpb.Schedule](ctx, listSchedulesRequest, s.schedulerServiceClient.ListSchedules)
	if err != nil {
		return nil, err
	}
	listJobsRequest := &schedulerservicepb.ListJobsRequest{Parent: parent, Filter: pendingOneOffFilter(agentRn), OrderBy: "schedule_time"}
	jobs, err := aip.Paginate[*schedulerpb.Job](ctx, listJobsRequest, s.schedulerServiceClient.ListJobs)
	if err != nil {
		return nil, err
	}

	var lines []string
	for _, schedule := range schedules {
		when := fmt.Sprintf("recurring, cron %q in %s, paused", schedule.GetCron(), schedule.GetTimeZone())
		if schedule.GetNextScheduleTime() != nil {
			when = fmt.Sprintf("recurring, cron %q in %s, next run at %s", schedule.GetCron(), schedule.GetTimeZone(), schedule.GetNextScheduleTime().AsTime().Format(time.RFC3339))
		}
		lines = append(lines, fmt.Sprintf("- %s: %s; %s", schedule.GetName(), when, describeWake(schedule.GetPayload())))
	}
	for _, job := range jobs {
		lines = append(lines, fmt.Sprintf("- %s: once, at %s; %s", job.GetName(), job.GetScheduleTime().AsTime().Format(time.RFC3339), describeWake(job.GetPayload())))
	}
	if len(lines) == 0 {
		return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), "You have no schedules."), nil
	}
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), strings.Join(lines, "\n")), nil
}

// describeWake describes what a run of a schedule does.
func describeWake(payload *anypb.Any) string {
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{}
	if err := payload.UnmarshalTo(wakeAgentRequest); err != nil {
		return "unreadable payload"
	}
	if subAgent := wakeAgentRequest.GetSubAgent(); subAgent != nil {
		return fmt.Sprintf("launches sub-agent %q with task %q", subAgent.GetTitle(), subAgent.GetTask())
	}
	return fmt.Sprintf("wakes you with %q", wakeAgentRequest.GetMessage())
}

// ============================== cancel_schedule =============================

const cancelScheduleToolDescription = "Cancel one of your schedules by the name schedule or list_schedules gave: " +
	"a recurring schedule stops for good, a one-off run never happens."

func cancelSchedule(ctx context.Context, s *Service, agent *agentpb.Agent, toolCall *aipb.ToolCall, arguments *agentservicepb.CancelScheduleToolArguments) (*aipb.ToolResult, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	name := arguments.GetName()
	// Checked before reading it, so an agent learns nothing of other users' schedules.
	if !strings.HasPrefix(name, agentRn.UserRn().String()+"/") {
		return nil, notOwnScheduleError(name)
	}
	switch {
	case schedulerpb.MatchUserScheduleRn(name):
		getScheduleRequest := &schedulerservicepb.GetScheduleRequest{Name: name}
		schedule, err := s.schedulerServiceClient.GetSchedule(ctx, getScheduleRequest)
		if err != nil {
			return nil, err
		}
		if schedule.GetLabels()[agentpb.Labels.Agent.GetKey()] != agentRn.Agent {
			return nil, notOwnScheduleError(name)
		}
		deleteScheduleRequest := &schedulerservicepb.DeleteScheduleRequest{Name: name, Etag: schedule.GetEtag()}
		if _, err := s.schedulerServiceClient.DeleteSchedule(ctx, deleteScheduleRequest); err != nil {
			return nil, err
		}
	case schedulerpb.MatchUserJobRn(name):
		getJobRequest := &schedulerservicepb.GetJobRequest{Name: name}
		job, err := s.schedulerServiceClient.GetJob(ctx, getJobRequest)
		if err != nil {
			return nil, err
		}
		if job.GetLabels()[agentpb.Labels.Agent.GetKey()] != agentRn.Agent {
			return nil, notOwnScheduleError(name)
		}
		cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: name}
		if _, err := s.schedulerServiceClient.CancelJob(ctx, cancelJobRequest); err != nil {
			return nil, err
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a schedule's name: use one schedule or list_schedules gave", name).Err()
	}
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), fmt.Sprintf("Cancelled %s.", name)), nil
}

// notOwnScheduleError refuses a schedule or job that is not the agent's own: under its user and labeled with its id.
func notOwnScheduleError(name string) error {
	return status.Errorf(codes.PermissionDenied, "%s is not one of your schedules", name).Err()
}
