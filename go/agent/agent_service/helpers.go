package agent_service

import (
	"context"
	"fmt"
	"strings"

	"go.einride.tech/aip/resourcename"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/gengo/agent/model"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/uuid"
)

// buildUpdateClause renders "col1 = $1, col2 = $2, ..." matching the order in
// which postgres.GetParams extracts the columns.
func buildUpdateClause(columns []string) string {
	parts := make([]string, len(columns))
	for i, column := range columns {
		parts[i] = fmt.Sprintf("%s = $%d", column, i+1)
	}
	return strings.Join(parts, ", ")
}

// saveTask persists the given columns of a task, refreshing update_time
// (the runner heartbeat) and etag.
func (s *Service) saveTask(ctx context.Context, task *agentpb.Task, columns ...string) error {
	task.UpdateTime = timestamppb.Now()
	var err error
	if task.Etag, err = aip.ComputeETag(task); err != nil {
		return fmt.Errorf("computing etag: %w", err)
	}
	taskModel, err := model.TaskFromPb(task)
	if err != nil {
		return fmt.Errorf("converting task to model: %w", err)
	}
	columns = append(columns, "update_time", "etag")
	if _, err := s.agentPostgresStore.UpdateTask(ctx, taskModel, buildUpdateClause(columns), columns, ""); err != nil {
		return fmt.Errorf("updating task: %w", err)
	}
	return nil
}

// saveAgent persists the given columns of an agent, refreshing update_time and etag.
func (s *Service) saveAgent(ctx context.Context, agent *agentpb.Agent, columns ...string) error {
	agent.UpdateTime = timestamppb.Now()
	var err error
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return fmt.Errorf("computing etag: %w", err)
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return fmt.Errorf("converting agent to model: %w", err)
	}
	columns = append(columns, "update_time", "etag")
	if _, err := s.agentPostgresStore.UpdateAgent(ctx, agentModel, buildUpdateClause(columns), columns, ""); err != nil {
		return fmt.Errorf("updating agent: %w", err)
	}
	return nil
}

// getAgentByName loads an agent pb by resource name.
func (s *Service) getAgentByName(ctx context.Context, name string) (*agentpb.Agent, error) {
	organizationID, userID, agentID, err := model.ParseAgentName(name)
	if err != nil {
		return nil, err
	}
	agentModel, err := s.agentPostgresStore.GetAgent(ctx, organizationID, userID, agentID)
	if err != nil {
		return nil, err
	}
	return agentModel.ToPb()
}

// getTaskByName loads a task pb by resource name.
func (s *Service) getTaskByName(ctx context.Context, name string) (*agentpb.Task, error) {
	organizationID, userID, agentID, taskID, err := model.ParseTaskName(name)
	if err != nil {
		return nil, err
	}
	taskModel, err := s.agentPostgresStore.GetTask(ctx, organizationID, userID, agentID, taskID)
	if err != nil {
		return nil, err
	}
	return taskModel.ToPb()
}

// userNameOf extracts "organizations/{o}/users/{u}" from any resource name below a user.
func userNameOf(name string) (string, error) {
	var organizationID, userID string
	if err := resourcename.Sscan(name, "organizations/{organization}/users/{user}", &organizationID, &userID); err != nil {
		// Sscan requires exact pattern; fall back to manual split.
		segments := strings.Split(name, "/")
		if len(segments) < 4 || segments[0] != "organizations" || segments[2] != "users" {
			return "", fmt.Errorf("cannot extract user from %q: %v", name, err)
		}
		return strings.Join(segments[:4], "/"), nil
	}
	return resourcename.Sprint("organizations/{organization}/users/{user}", organizationID, userID), nil
}

// createChat creates a fresh backing chat for the given owner resource.
func (s *Service) createChat(ctx context.Context, ownerName, title string) (string, error) {
	userName, err := userNameOf(ownerName)
	if err != nil {
		return "", err
	}
	chat, err := s.aiServiceClient.CreateChat(ctx, &aiservicepb.CreateChatRequest{
		Parent:    userName,
		Chat:      &aipb.Chat{Title: title},
		RequestId: uuid.MustNewV7().String(),
	})
	if err != nil {
		return "", fmt.Errorf("creating chat: %w", err)
	}
	return chat.Name, nil
}

// textMessage builds a single-text-block message.
func textMessage(role aipb.Role, text string) *aipb.Message {
	return &aipb.Message{Role: role, Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: text}}}}
}

// toolResultMessage builds a TOOL message from the given results.
func toolResultMessage(results []*aipb.ToolResult) *aipb.Message {
	blocks := make([]*aipb.Block, len(results))
	for i, result := range results {
		blocks[i] = &aipb.Block{Content: &aipb.Block_ToolResult{ToolResult: result}}
	}
	return &aipb.Message{Role: aipb.Role_ROLE_TOOL, Blocks: blocks}
}

// appendMessage appends a message to a chat.
func (s *Service) appendMessage(ctx context.Context, chatName string, message *aipb.Message) (*aipb.Message, error) {
	return s.aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{
		Parent:    chatName,
		Message:   message,
		RequestId: uuid.MustNewV7().String(),
	})
}

// messageText concatenates the text blocks of a message.
func messageText(message *aipb.Message) string {
	var parts []string
	for _, block := range message.GetBlocks() {
		if text := block.GetText(); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

// toolCallsOf extracts the tool calls of a message.
func toolCallsOf(message *aipb.Message) []*aipb.ToolCall {
	var calls []*aipb.ToolCall
	for _, block := range message.GetBlocks() {
		if call := block.GetToolCall(); call != nil {
			calls = append(calls, call)
		}
	}
	return calls
}

// newResourceID mints a system-generated resource id.
func newResourceID() string {
	return aip.NewSystemGeneratedBase32ResourceID()
}
