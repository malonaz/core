// Package mock implements a scriptable in-process AI provider for system
// acceptance tests. Tests attach a script to a message annotation; each
// generation turn replays the next scripted assistant message.
package mock

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const (
	// ScriptAnnotationKey holds the script on a message: a JSON array of
	// ai.v1.Message whose blocks are replayed, one message per generation turn.
	ScriptAnnotationKey = "mock.malonaz.com/script"

	// ToolsPlaceholder, when present in a scripted text block, is replaced with
	// the comma-joined names of the tools visible to the provider. Tests use it
	// to assert the provider-visible tool list (e.g. prompt-cache stability).
	ToolsPlaceholder = "${TOOLS}"
)

// Script marshals scripted assistant messages into the annotation value.
func Script(messages ...*aipb.Message) (string, error) {
	parts := make([]string, 0, len(messages))
	for _, message := range messages {
		bytes, err := pbutil.JSONMarshal(message)
		if err != nil {
			return "", err
		}
		parts = append(parts, string(bytes))
	}
	return "[" + strings.Join(parts, ",") + "]", nil
}

// Client is a scriptable GenerateMessageClient.
type Client struct{}

// NewClient returns a new mock provider client.
func NewClient() *Client { return &Client{} }

// ProviderId implements the provider.Provider interface.
func (c *Client) ProviderId() string { return provider.Mock }

// Start implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Stop implements the provider.Provider interface.
func (c *Client) Stop() {}

// StreamGenerateMessage implements the provider.GenerateMessageClient
// interface by replaying the scripted assistant message for the current turn.
func (c *Client) StreamGenerateMessage(
	ctx context.Context,
	request *aiservicepb.GenerateMessageRequest,
	messages []*aipb.Message,
	sender *provider.AsyncMessageContentSender,
) error {
	scriptedMessage, err := nextScriptedMessage(messages)
	if err != nil {
		return err
	}

	stopReason := aiservicepb.StopReason_STOP_REASON_END_TURN
	for _, block := range scriptedMessage.GetBlocks() {
		if text := block.GetText(); strings.Contains(text, ToolsPlaceholder) {
			block.Content = &aipb.Block_Text{Text: strings.ReplaceAll(text, ToolsPlaceholder, joinToolNames(request.GetTools()))}
		}
		if block.GetToolCall() != nil {
			stopReason = aiservicepb.StopReason_STOP_REASON_TOOL_CALL
		}
		sender.SendBlocks(ctx, block)
	}
	sender.SendModelUsage(ctx, &aipb.ModelUsage{
		Model:       request.GetModel(),
		InputToken:  &aipb.ResourceConsumption{Quantity: 100},
		OutputToken: &aipb.ResourceConsumption{Quantity: 10},
	})
	sender.SendStopReason(ctx, stopReason)
	return nil
}

// nextScriptedMessage locates the most recent scripted message in the
// conversation and returns the scripted turn indexed by the number of
// assistant messages generated since.
func nextScriptedMessage(messages []*aipb.Message) (*aipb.Message, error) {
	scriptIndex := -1
	for i, message := range messages {
		if _, ok := message.GetAnnotations()[ScriptAnnotationKey]; ok {
			scriptIndex = i
		}
	}
	if scriptIndex == -1 {
		return nil, status.Errorf(codes.InvalidArgument, "mock provider: no message carries the %q annotation", ScriptAnnotationKey).Err()
	}

	scriptedMessages, err := pbutil.JSONUnmarshalSlice[aipb.Message](pbutil.JsonUnmarshalStrictOptions, []byte(messages[scriptIndex].GetAnnotations()[ScriptAnnotationKey]))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "mock provider: parsing script: %v", err).Err()
	}

	turn := 0
	for _, message := range messages[scriptIndex+1:] {
		if message.GetRole() == aipb.Role_ROLE_ASSISTANT {
			turn++
		}
	}
	if turn >= len(scriptedMessages) {
		return nil, status.Errorf(codes.InvalidArgument, "mock provider: script exhausted: turn %d of %d", turn, len(scriptedMessages)).Err()
	}
	return scriptedMessages[turn], nil
}

func joinToolNames(tools []*aipb.Tool) string {
	toolNames := make([]string, 0, len(tools))
	for _, tool := range tools {
		toolNames = append(toolNames, tool.GetName())
	}
	return strings.Join(toolNames, ",")
}

// Verify interface compliance at compile time.
var _ provider.GenerateMessageClient = (*Client)(nil)
