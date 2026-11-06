package agent_service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// scheduleCall returns a call of the schedule tool.
func scheduleCall(id string, arguments map[string]any) *aipb.ToolCall {
	arguments["tool_call_title"] = "Schedule"
	return newToolCall(id, scheduleToolName, arguments)
}

// cancelScheduleCall returns a call of the cancel_schedule tool.
func cancelScheduleCall(id, name string) *aipb.ToolCall {
	return newToolCall(id, cancelScheduleToolName, map[string]any{"tool_call_title": "Cancel", "name": name})
}

// subAgentArguments are the arguments of an ad-hoc sub-agent launch.
func subAgentArguments() map[string]any {
	return map[string]any{"title": "digest", "task": "summarize the inbox", "instructions": "You summarize."}
}

// mustSchedule schedules through the tool, failing the test on an error result.
func mustSchedule(t *testing.T, service *testReplica, agent *agentpb.Agent, id string, arguments map[string]any) *aipb.ToolResult {
	t.Helper()
	toolResult := callBuiltinTool(t, service, agent, scheduleCall(id, arguments))
	require.Nil(t, toolResult.GetError(), toolResult.GetError().GetMessage())
	return toolResult
}

// wakeAgentRequest unpacks a schedule's or job's payload.
func wakeAgentRequest(t *testing.T, payload *anypb.Any) *agentservicepb.WakeAgentRequest {
	t.Helper()
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{}
	require.NoError(t, payload.UnmarshalTo(wakeAgentRequest))
	return wakeAgentRequest
}

// scheduleLabels returns the labels of an agent's schedules waking it a given way.
func scheduleLabels(t *testing.T, agent *agentpb.Agent, wake string) map[string]string {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	return map[string]string{agentpb.Labels.Agent.GetKey(): agentRn.Agent, agentpb.Labels.Wake.GetKey(): wake}
}

// userOf returns the user owning an agent.
func userOf(t *testing.T, agent *agentpb.Agent) string {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	return agentRn.UserRn().String()
}

// requireJobState checks a job's state in the fake scheduler.
func requireJobState(t *testing.T, service *testReplica, name string, state schedulerpb.JobState) {
	t.Helper()
	getJobRequest := &schedulerservicepb.GetJobRequest{Name: name}
	job, err := service.scheduler.GetJob(asTestService(), getJobRequest)
	require.NoError(t, err)
	require.Equal(t, state, job.State, name)
}

func TestSchedule_Recurring(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	arguments := func() map[string]any {
		return map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "send the standup"}
	}

	toolResult := mustSchedule(t, service, agent, "call-1", arguments())
	// A replayed call creates nothing twice.
	mustSchedule(t, service, agent, "call-1", arguments())

	schedules := service.scheduler.schedulesOf(t, agent.Name)
	require.Len(t, schedules, 1)
	schedule := schedules[0]
	require.True(t, schedulerpb.MatchUserScheduleRn(schedule.Name))
	parent, err := scheduleParent(schedule.Name)
	require.NoError(t, err)
	require.Equal(t, userOf(t, agent), parent)
	require.Equal(t, "0 9 * * *", schedule.Cron)
	require.Equal(t, "Europe/Paris", schedule.TimeZone)
	require.Equal(t, scheduleLabels(t, agent, agentpb.Labels.Wake.Self), schedule.Labels)
	payload := wakeAgentRequest(t, schedule.Payload)
	require.Equal(t, agent.Name, payload.Name)
	require.Equal(t, "send the standup", payload.GetMessage())
	require.Contains(t, toolResult.GetContent(), schedule.Name)
	require.Empty(t, service.scheduler.jobsOf(t, agent.Name))

	// A recurring sub-agent launch is labeled as such.
	subAgentSchedule := arguments()
	delete(subAgentSchedule, "message")
	subAgentSchedule["sub_agent"] = subAgentArguments()
	mustSchedule(t, service, agent, "call-2", subAgentSchedule)
	schedules = service.scheduler.schedulesOf(t, agent.Name)
	require.Len(t, schedules, 2)
	require.Equal(t, scheduleLabels(t, agent, agentpb.Labels.Wake.SubAgent), schedules[1].Labels)
	require.Equal(t, "summarize the inbox", wakeAgentRequest(t, schedules[1].Payload).GetSubAgent().GetTask())
}

func TestSchedule_OneOff(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	scheduleTime := time.Now().Add(48 * time.Hour).Truncate(time.Second).UTC()
	delayArguments := func() map[string]any { return map[string]any{"delay": "3600s", "message": "check the deploy"} }
	timeArguments := func() map[string]any {
		return map[string]any{"time": scheduleTime.Format(time.RFC3339), "sub_agent": subAgentArguments()}
	}

	before := time.Now()
	delayResult := mustSchedule(t, service, agent, "call-1", delayArguments())
	timeResult := mustSchedule(t, service, agent, "call-2", timeArguments())
	// Replayed calls create nothing twice.
	mustSchedule(t, service, agent, "call-1", delayArguments())
	mustSchedule(t, service, agent, "call-2", timeArguments())

	jobs := service.scheduler.jobsOf(t, agent.Name)
	require.Len(t, jobs, 2)
	delayJob, timeJob := jobs[0], jobs[1]
	for _, job := range jobs {
		require.True(t, schedulerpb.MatchUserJobRn(job.Name))
		parent, err := jobParent(job.Name)
		require.NoError(t, err)
		require.Equal(t, userOf(t, agent), parent)
		require.Equal(t, schedulerpb.JobState_JOB_STATE_PENDING, job.State)
		require.Empty(t, job.Schedule)
	}
	require.WithinRange(t, delayJob.ScheduleTime.AsTime(), before.Add(time.Hour), time.Now().Add(time.Hour))
	require.Equal(t, scheduleLabels(t, agent, agentpb.Labels.Wake.Self), delayJob.Labels)
	require.Equal(t, "check the deploy", wakeAgentRequest(t, delayJob.Payload).GetMessage())
	require.Contains(t, delayResult.GetContent(), delayJob.Name)

	require.True(t, scheduleTime.Equal(timeJob.ScheduleTime.AsTime()))
	require.Equal(t, scheduleLabels(t, agent, agentpb.Labels.Wake.SubAgent), timeJob.Labels)
	require.Equal(t, "digest", wakeAgentRequest(t, timeJob.Payload).GetSubAgent().GetTitle())
	require.Contains(t, timeResult.GetContent(), timeJob.Name)
	require.Empty(t, service.scheduler.schedulesOf(t, agent.Name))
}

func TestSchedule_InvalidArguments(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	// A root agent that may launch no sub-agent.
	leafAgent := createTestAgentWithSpec(t, service, userOf(t, agent), &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel})

	for _, testCase := range []struct {
		name      string
		agent     *agentpb.Agent
		arguments map[string]any
		code      codes.Code
	}{
		{name: "cron without time zone", agent: agent, arguments: map[string]any{"cron": "0 9 * * *", "message": "hi"}, code: codes.InvalidArgument},
		{name: "time in the past", agent: agent, arguments: map[string]any{"time": time.Now().Add(-time.Hour).Format(time.RFC3339), "message": "hi"}, code: codes.InvalidArgument},
		{name: "no wake", agent: agent, arguments: map[string]any{"delay": "60s"}, code: codes.InvalidArgument},
		{name: "sub-agent at depth 0", agent: leafAgent, arguments: map[string]any{"delay": "60s", "sub_agent": subAgentArguments()}, code: codes.PermissionDenied},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			toolResult := callBuiltinTool(t, service, testCase.agent, scheduleCall("call-"+testCase.name, testCase.arguments))
			require.Equal(t, int32(testCase.code), toolResult.GetError().GetCode(), toolResult.GetError().GetMessage())
		})
	}
	for _, scheduledAgent := range []*agentpb.Agent{agent, leafAgent} {
		require.Empty(t, service.scheduler.schedulesOf(t, scheduledAgent.Name))
		require.Empty(t, service.scheduler.jobsOf(t, scheduledAgent.Name))
	}
}

func TestListSchedules_ListsOnlyTheAgentsOwn(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	siblingAgent := createTestAgentWithSpec(t, service, userOf(t, agent), &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel})
	strangerAgent := createTestAgent(t, service)
	listCall := newToolCall("call-list", listSchedulesToolName, map[string]any{"tool_call_title": "List"})

	require.Equal(t, "You have no schedules.", callBuiltinTool(t, service, agent, listCall).GetContent())

	mustSchedule(t, service, agent, "call-1", map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "standup"})
	mustSchedule(t, service, agent, "call-2", map[string]any{"delay": "3600s", "sub_agent": subAgentArguments()})
	mustSchedule(t, service, siblingAgent, "call-1", map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "sibling"})
	mustSchedule(t, service, siblingAgent, "call-2", map[string]any{"delay": "3600s", "message": "sibling"})
	mustSchedule(t, service, strangerAgent, "call-1", map[string]any{"delay": "3600s", "message": "stranger"})
	schedule := service.scheduler.schedulesOf(t, agent.Name)[0]
	oneOffJob := service.scheduler.jobsOf(t, agent.Name)[0]
	// A tick of a recurring schedule is not a one-off.
	tickJob := service.scheduler.tick(t, schedule.Name)

	toolResult := callBuiltinTool(t, service, agent, listCall)

	require.Nil(t, toolResult.GetError())
	content := toolResult.GetContent()
	require.Contains(t, content, fmt.Sprintf("- %s: recurring, cron %q in Europe/Paris, next run at", schedule.Name, "0 9 * * *"))
	require.Contains(t, content, `wakes you with "standup"`)
	require.Contains(t, content, fmt.Sprintf("- %s: once, at", oneOffJob.Name))
	require.Contains(t, content, `launches sub-agent "digest" with task "summarize the inbox"`)
	require.NotContains(t, content, tickJob.Name)
	require.NotContains(t, content, "sibling")
	require.NotContains(t, content, "stranger")
}

func TestCancelSchedule(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	siblingAgent := createTestAgentWithSpec(t, service, userOf(t, agent), &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel})
	strangerAgent := createTestAgent(t, service)
	recurring := map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "standup"}
	oneOff := map[string]any{"delay": "3600s", "message": "later"}
	for _, scheduledAgent := range []*agentpb.Agent{agent, siblingAgent, strangerAgent} {
		mustSchedule(t, service, scheduledAgent, "call-1", recurring)
		mustSchedule(t, service, scheduledAgent, "call-2", oneOff)
	}
	schedule := service.scheduler.schedulesOf(t, agent.Name)[0]
	job := service.scheduler.jobsOf(t, agent.Name)[0]

	toolResult := callBuiltinTool(t, service, agent, cancelScheduleCall("call-3", schedule.Name))
	require.Nil(t, toolResult.GetError(), toolResult.GetError().GetMessage())
	require.Empty(t, service.scheduler.schedulesOf(t, agent.Name))
	toolResult = callBuiltinTool(t, service, agent, cancelScheduleCall("call-4", job.Name))
	require.Nil(t, toolResult.GetError(), toolResult.GetError().GetMessage())
	requireJobState(t, service, job.Name, schedulerpb.JobState_JOB_STATE_CANCELLED)

	// Another agent's, of the same user or not, are refused and left alone.
	for _, otherAgent := range []*agentpb.Agent{siblingAgent, strangerAgent} {
		otherSchedule := service.scheduler.schedulesOf(t, otherAgent.Name)[0]
		otherJob := service.scheduler.jobsOf(t, otherAgent.Name)[0]
		toolResult = callBuiltinTool(t, service, agent, cancelScheduleCall("call-5", otherSchedule.Name))
		require.Equal(t, int32(codes.PermissionDenied), toolResult.GetError().GetCode(), toolResult.GetError().GetMessage())
		toolResult = callBuiltinTool(t, service, agent, cancelScheduleCall("call-6", otherJob.Name))
		require.Equal(t, int32(codes.PermissionDenied), toolResult.GetError().GetCode(), toolResult.GetError().GetMessage())
		require.Len(t, service.scheduler.schedulesOf(t, otherAgent.Name), 1)
		requireJobState(t, service, otherJob.Name, schedulerpb.JobState_JOB_STATE_PENDING)
	}

	toolResult = callBuiltinTool(t, service, agent, cancelScheduleCall("call-7", agent.Name))
	require.Equal(t, int32(codes.InvalidArgument), toolResult.GetError().GetCode())
}

func TestWakeAgent_DeliversOncePerJob(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("done"))
	agent := createTestAgent(t, service)
	arguments := func() map[string]any { return map[string]any{"delay": "3600s", "message": "check the deploy"} }
	mustSchedule(t, service, agent, "call-1", arguments())
	mustSchedule(t, service, agent, "call-2", arguments())
	jobs := service.scheduler.jobsOf(t, agent.Name)
	require.Len(t, jobs, 2)
	job, nextJob := jobs[0], jobs[1]
	sentCount := func() int { return len(sentMessages(t, service, agent.Name)) }

	// A replayed run, before and after its message is delivered, delivers it once.
	for range 2 {
		_, err := service.scheduler.run(t, service, job.Payload, job.Name)
		require.NoError(t, err)
	}
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	_, err := service.scheduler.run(t, service, job.Payload, job.Name)
	require.NoError(t, err)
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, 1, sentCount())

	// Another run delivers again.
	_, err = service.scheduler.run(t, service, nextJob.Payload, nextJob.Name)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sentCount() == 2 }, waitTimeout, waitTick)
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, 2, sentCount())
}

func TestWakeAgent_RunsOnlyAsAJob(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{Name: agent.Name, Wake: &agentservicepb.WakeAgentRequest_Message{Message: "hi"}}

	_, err := service.WakeAgent(context.Background(), wakeAgentRequest)

	require.True(t, status.HasCode(err, codes.FailedPrecondition), err)
	require.Empty(t, sentMessages(t, service, agent.Name))
}

func TestWakeAgent_WakesItself(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	mustSchedule(t, service, agent, "call-1", map[string]any{"delay": "3600s", "message": "check the deploy"})
	job := service.scheduler.jobsOf(t, agent.Name)[0]
	service.aiService.script(agent.Chat, textGeneration("the deploy is green"))

	_, err := service.scheduler.run(t, service, job.Payload, job.Name)

	require.NoError(t, err)
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	messages := deliveredMessages(t, service, agent.Name)
	require.Len(t, messages, 1)
	sender, _ := agentpb.Annotations.Sender.Get(messages[0])
	require.Equal(t, agent.Name, sender)
	require.Equal(t, []string{"[Your scheduled message]", "check the deploy"}, deliveredTexts(t, service, agent.Chat, messages[0].Name))
	require.Len(t, service.aiService.generateMessageRequests(agent.Chat), 1)
}

func TestWakeAgent_LaunchesAFreshSubAgent(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("inbox summarized"))
	agent := createTestAgent(t, service)
	mustSchedule(t, service, agent, "call-1", map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "sub_agent": subAgentArguments()})
	tickJob := service.scheduler.tick(t, service.scheduler.schedulesOf(t, agent.Name)[0].Name)

	// A replayed run launches one sub-agent.
	for range 2 {
		_, err := service.scheduler.run(t, service, tickJob.Payload, tickJob.Name)
		require.NoError(t, err)
	}

	// The sub-agent's report wakes the parent.
	require.Eventually(t, func() bool {
		subAgents := listChildAgents(t, service, agent)
		if len(subAgents) != 1 || subAgents[0].GetLatestTurn().GetOutcome() != agentpb.Turn_OUTCOME_SUCCEEDED {
			return false
		}
		return len(deliveredMessages(t, service, agent.Name)) == 1
	}, waitTimeout, waitTick)
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	subAgents := listChildAgents(t, service, agent)
	require.Len(t, subAgents, 1)
	subAgent := subAgents[0]
	require.Equal(t, agent.Name, subAgent.ParentAgent)
	require.Equal(t, "digest", subAgent.Title)
	require.Equal(t, "You summarize.", subAgent.Spec.Instructions)

	taskMessages := sentMessages(t, service, subAgent.Name)
	require.Len(t, taskMessages, 1)
	sender, _ := agentpb.Annotations.Sender.Get(taskMessages[0])
	require.Equal(t, agent.Name, sender)
	require.Equal(t, "summarize the inbox", lastText(taskMessages[0]))

	reportMessage := deliveredMessages(t, service, agent.Name)[0]
	sender, _ = agentpb.Annotations.Sender.Get(reportMessage)
	require.Equal(t, subAgent.Name, sender)
	require.Equal(t, []string{fmt.Sprintf("[Report from sub-agent %s (%q)]", subAgent.Name, subAgent.Title), "inbox summarized"}, deliveredTexts(t, service, agent.Chat, reportMessage.Name))

	// The next tick launches a fresh sub-agent.
	nextTickJob := service.scheduler.tick(t, service.scheduler.schedulesOf(t, agent.Name)[0].Name)
	_, err := service.scheduler.run(t, service, nextTickJob.Payload, nextTickJob.Name)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(deliveredMessages(t, service, agent.Name)) == 2 }, waitTimeout, waitTick)
	require.Len(t, listChildAgents(t, service, agent), 2)
	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
}

func TestWakeAgent_FreshRunRespectsTheDepth(t *testing.T) {
	service := newTestService(t)
	leafAgent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel})
	// Whoever wrote the payload, the run checks the agent may launch it.
	wakeAgentRequest := &agentservicepb.WakeAgentRequest{
		Name: leafAgent.Name,
		Wake: &agentservicepb.WakeAgentRequest_SubAgent{SubAgent: &agentservicepb.CreateAgentsToolArguments_SubAgent{
			Title: "digest", Task: "summarize the inbox", Instructions: "You summarize.",
		}},
	}
	payload, err := anypb.New(wakeAgentRequest)
	require.NoError(t, err)
	jobRn, err := schedulerpb.NewJobRn(userOf(t, leafAgent), "forged")
	require.NoError(t, err)

	_, err = service.scheduler.run(t, service, payload, jobRn.String())

	require.True(t, status.HasCode(err, codes.PermissionDenied), err)
	require.Empty(t, listChildAgents(t, service, leafAgent))
}

func TestDeleteAgent_CancelsSchedules(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	siblingAgent := createTestAgentWithSpec(t, service, userOf(t, agent), &agentpb.AgentSpec{Instructions: "Be helpful.", Model: testModel})
	for _, scheduledAgent := range []*agentpb.Agent{agent, siblingAgent} {
		mustSchedule(t, service, scheduledAgent, "call-1", map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "standup"})
		mustSchedule(t, service, scheduledAgent, "call-2", map[string]any{"delay": "3600s", "message": "later"})
	}
	tickJob := service.scheduler.tick(t, service.scheduler.schedulesOf(t, agent.Name)[0].Name)
	jobs := service.scheduler.jobsOf(t, agent.Name)
	require.Len(t, jobs, 2)

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)

	require.Empty(t, service.scheduler.schedulesOf(t, agent.Name))
	for _, job := range jobs {
		requireJobState(t, service, job.Name, schedulerpb.JobState_JOB_STATE_CANCELLED)
	}
	require.Contains(t, []string{jobs[0].Name, jobs[1].Name}, tickJob.Name)
	require.Len(t, service.scheduler.schedulesOf(t, siblingAgent.Name), 1)
	siblingJobs := service.scheduler.jobsOf(t, siblingAgent.Name)
	require.Len(t, siblingJobs, 1)
	requireJobState(t, service, siblingJobs[0].Name, schedulerpb.JobState_JOB_STATE_PENDING)
}

// scheduleBoth gives an agent a recurring schedule and a one-off run, returning the run.
func scheduleBoth(t *testing.T, service *testReplica, agent *agentpb.Agent) *schedulerpb.Job {
	t.Helper()
	mustSchedule(t, service, agent, "call-1", map[string]any{"cron": "0 9 * * *", "time_zone": "Europe/Paris", "message": "standup"})
	mustSchedule(t, service, agent, "call-2", map[string]any{"delay": "3600s", "message": "later"})
	return service.scheduler.jobsOf(t, agent.Name)[0]
}

// requireScheduled checks whether an agent's schedule and one-off run are still pending.
func requireScheduled(t *testing.T, service *testReplica, agent *agentpb.Agent, job *schedulerpb.Job, scheduled bool) {
	t.Helper()
	jobState := schedulerpb.JobState_JOB_STATE_CANCELLED
	if scheduled {
		jobState = schedulerpb.JobState_JOB_STATE_PENDING
	}
	require.Equal(t, scheduled, len(service.scheduler.schedulesOf(t, agent.Name)) == 1, agent.Name)
	requireJobState(t, service, job.Name, jobState)
}

func TestDeleteAgent_CancelsSchedulesUnderTheCallersSession(t *testing.T) {
	service := newAuthenticatedTestService(t)
	alice := newTestUser(t)
	aliceCtx := service.as(t, alice, false)
	agent, err := service.createAgent(aliceCtx, alice, nil)
	require.NoError(t, err)
	job := scheduleBoth(t, service.testReplica, agent)

	// Through the server, whose interceptors put alice's signed session on the service's outgoing calls.
	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err = service.client.DeleteAgent(aliceCtx, deleteAgentRequest)

	require.NoError(t, err)
	require.NotNil(t, getAgent(t, service.testReplica, agent.Name).DeleteTime)
	requireScheduled(t, service.testReplica, agent, job, false)
}

func TestDeleteAgent_ForceCancelsTheSubtreesSchedules(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)
	subSubAgent := createTestSubAgent(t, service, subAgent)
	siblingAgent := createTestSubAgent(t, service, agent)
	agents := []*agentpb.Agent{agent, subAgent, subSubAgent, siblingAgent}
	jobs := make([]*schedulerpb.Job, len(agents))
	for i, scheduledAgent := range agents {
		jobs[i] = scheduleBoth(t, service, scheduledAgent)
	}

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name, Force: true}
	deletedAgent, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)

	for i, deletedAgent := range agents {
		requireScheduled(t, service, deletedAgent, jobs[i], false)
	}
	// One delete: the whole subtree at the same time.
	for _, descendant := range agents[1:] {
		require.True(t, deletedAgent.DeleteTime.AsTime().Equal(getAgent(t, service, descendant.Name).DeleteTime.AsTime()), descendant.Name)
	}
}

func TestDeleteAgent_CancellationFailureDeletesNothing(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)
	jobs := []*schedulerpb.Job{scheduleBoth(t, service, agent), scheduleBoth(t, service, subAgent)}
	service.scheduler.failCancelJobs(status.Errorf(codes.Unavailable, "scheduler down").Err())

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name, Force: true}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)

	require.True(t, status.HasCode(err, codes.Unavailable), err)
	for _, liveAgent := range []*agentpb.Agent{agent, subAgent} {
		require.Nil(t, getAgent(t, service, liveAgent.Name).DeleteTime, liveAgent.Name)
	}

	// A retry repeats the cleanup.
	service.scheduler.failCancelJobs(nil)
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	for i, deletedAgent := range []*agentpb.Agent{agent, subAgent} {
		require.NotNil(t, getAgent(t, service, deletedAgent.Name).DeleteTime, deletedAgent.Name)
		requireScheduled(t, service, deletedAgent, jobs[i], false)
	}
}
