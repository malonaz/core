package agent_service

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	workspaceservicepb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/health"
)

const (
	workspaceService = "malonaz.workspace.workspace_service.v1.WorkspaceService"
	// workspaceDescriptorSet is the descriptor set the workspace server serves reflection from, comments included.
	workspaceDescriptorSet = "cmd/workspace-server/descriptor_set.bin"
)

// newTestWorkspaceTarget serves the WorkspaceService as the workspace server does, reflection from its descriptor set,
// and returns its endpoint.
func newTestWorkspaceTarget(t *testing.T) string {
	t.Helper()
	descriptorSet, err := os.ReadFile(workspaceDescriptorSet)
	require.NoError(t, err)
	socketDirectory, err := os.MkdirTemp("", "workspace")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(socketDirectory) })
	serverOpts := &grpc.ServerOpts{
		Health:              &health.GRPCOpts{Interval: time.Second, Timeout: time.Second, Startup: time.Second},
		DisableTLS:          true,
		EnableReflection:    true,
		GracefulStopTimeout: 1,
		SocketPath:          filepath.Join(socketDirectory, "grpc.sock"),
	}
	server, err := grpc.NewServer(serverOpts, &certs.Opts{}, nil, "workspace", grpc.ServerOptions{FileDescriptorSet: descriptorSet}, func(server *grpc.Server) {
		workspaceservicepb.RegisterWorkspaceServiceServer(server.Raw, workspaceservicepb.UnimplementedWorkspaceServiceServer{})
	})
	require.NoError(t, err)
	go server.Serve(context.Background())
	t.Cleanup(func() { server.Stop() })
	return "unix:" + serverOpts.SocketPath
}

func TestToolCatalogue_SetsListedByNameEachCarryingItsError(t *testing.T) {
	_, healthEndpoint := newTestToolTarget(t)
	const unreachable = "test.Unreachable"
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{
			healthService:    healthEndpoint,
			workspaceService: newTestWorkspaceTarget(t),
			unreachable:      "unix:" + filepath.Join(t.TempDir(), "absent.sock"),
		}
	})

	response, err := service.ListToolSets(context.Background(), &agentservicepb.ListToolSetsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"toolSets/genui", healthToolSet, "toolSets/" + workspaceService, "toolSets/" + unreachable}, toolSetNames(response.ToolSets))
	// Comments travel with the reflection a descriptor set built with its source info serves.
	require.Contains(t, response.ToolSets[2].Description, "This API runs commands and edits files in one workspace")
	// An unreachable target is its set's error, not the list's.
	require.Nil(t, response.ToolSets[1].Error)
	require.Equal(t, int32(codes.Unavailable), response.ToolSets[3].Error.GetCode())

	firstPage, err := service.ListToolSets(context.Background(), &agentservicepb.ListToolSetsRequest{PageSize: 3})
	require.NoError(t, err)
	secondPage, err := service.ListToolSets(context.Background(), &agentservicepb.ListToolSetsRequest{PageSize: 3, PageToken: firstPage.NextPageToken})
	require.NoError(t, err)
	require.Equal(t, toolSetNames(response.ToolSets), append(toolSetNames(firstPage.ToolSets), toolSetNames(secondPage.ToolSets)...))
	require.Empty(t, secondPage.NextPageToken)

	toolSet, err := service.GetToolSet(context.Background(), &agentservicepb.GetToolSetRequest{Name: "toolSets/" + unreachable})
	require.NoError(t, err)
	require.Equal(t, int32(codes.Unavailable), toolSet.Error.GetCode())
	_, err = service.GetToolSet(context.Background(), &agentservicepb.GetToolSetRequest{Name: "toolSets/no.such.Service"})
	require.True(t, status.HasCode(err, codes.NotFound), "%v", err)
}

func TestToolCatalogue_ToolsListedByNameWithTheirFacts(t *testing.T) {
	_, healthEndpoint := newTestToolTarget(t)
	const unreachable = "test.Unreachable"
	service := newTestReplica(t, newFakeAiService(), func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		opts.ToolTargets = map[string]string{
			healthService:    healthEndpoint,
			workspaceService: newTestWorkspaceTarget(t),
			unreachable:      "unix:" + filepath.Join(t.TempDir(), "absent.sock"),
		}
	})

	// Streaming methods are no tools: Watch is absent.
	response, err := service.ListTools(context.Background(), &agentservicepb.ListToolsRequest{Parent: healthToolSet})
	require.NoError(t, err)
	require.Equal(t, []string{healthCheckGrant, healthToolSet + "/tools/List"}, toolNames(response.Tools))

	response, err = service.ListTools(context.Background(), &agentservicepb.ListToolsRequest{Parent: "toolSets/" + workspaceService})
	require.NoError(t, err)
	nameToTool := map[string]*agentpb.Tool{}
	for _, tool := range response.Tools {
		nameToTool[tool.Name] = tool
	}
	readFiles, exec := nameToTool["toolSets/"+workspaceService+"/tools/ReadFiles"], nameToTool["toolSets/"+workspaceService+"/tools/Exec"]
	require.Contains(t, readFiles.Description, "Read one or more text files.")
	require.True(t, readFiles.ReadOnly)
	require.False(t, exec.ReadOnly)
	require.False(t, exec.ResolvedByClient)

	// Across sets, an unreadable one is left out; on its own, it fails.
	response, err = service.ListTools(context.Background(), &agentservicepb.ListToolsRequest{Parent: "toolSets/-", PageSize: 1000})
	require.NoError(t, err)
	names := toolNames(response.Tools)
	require.True(t, slices.IsSorted(names))
	require.Contains(t, names, healthCheckGrant)
	require.Contains(t, names, "toolSets/genui/tools/Choice")
	_, err = service.ListTools(context.Background(), &agentservicepb.ListToolsRequest{Parent: "toolSets/" + unreachable})
	require.True(t, status.HasCode(err, codes.Unavailable), "%v", err)

	// Genui components change nothing; input components await a client's result.
	choice, err := service.GetTool(context.Background(), &agentservicepb.GetToolRequest{Name: "toolSets/genui/tools/Choice"})
	require.NoError(t, err)
	require.True(t, choice.ReadOnly)
	require.True(t, choice.ResolvedByClient)
	table, err := service.GetTool(context.Background(), &agentservicepb.GetToolRequest{Name: "toolSets/genui/tools/Table"})
	require.NoError(t, err)
	require.False(t, table.ResolvedByClient)
	_, err = service.GetTool(context.Background(), &agentservicepb.GetToolRequest{Name: healthToolSet + "/tools/Watch"})
	require.True(t, status.HasCode(err, codes.NotFound), "%v", err)
}

func TestToolCatalogue_SpecsGrantWhatItLists(t *testing.T) {
	service := newTestService(t)
	for _, grant := range []*agentpb.AgentSpec_ToolGrant{
		toolGrant("toolSets/genui/tools/NoSuchComponent"),
		{Grant: &agentpb.AgentSpec_ToolGrant_ToolSet{ToolSet: "toolSets/genui"}, Approval: agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED},
		toolGrant("toolSets/no.such.Service"),
	} {
		err := service.validateTools(&agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{grant}})
		require.True(t, status.HasCode(err, codes.InvalidArgument), "%v: %v", grant, err)
	}
	require.NoError(t, service.validateTools(&agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{toolGrant("toolSets/genui")}}))
	// A tool or set is granted at most once.
	twice := &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{toolGrant("toolSets/genui"), toolGrant("toolSets/genui")}}
	require.ErrorContains(t, protovalidate.Validate(twice), "agent_spec.unique_tools")
}

// toolSetNames returns the names of tool sets, in order.
func toolSetNames(toolSets []*agentpb.ToolSet) []string {
	names := make([]string, 0, len(toolSets))
	for _, toolSet := range toolSets {
		names = append(names, toolSet.Name)
	}
	return names
}

// toolNames returns the names of tools, in order.
func toolNames(tools []*agentpb.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}
