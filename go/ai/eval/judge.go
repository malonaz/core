package eval

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	evalpb "github.com/malonaz/core/genproto/ai/eval/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	verdictToolName = "record_verdicts"

	instructions = `You are an impartial judge of a conversation between an AI assistant and a user.
For each criterion, decide from the transcript alone whether the conversation meets it:
- pass: it does.
- fail: it does not.
- not-applicable: the criterion does not apply to this conversation.
Reason before you decide, citing the transcript, then record every verdict with the ` + verdictToolName + ` tool.`
)

var (
	verdictMapping = aip.NewEnumMapping[evalpb.Verdict]()
	// The verdicts a judge may reach, in the order it is offered them.
	verdicts = []evalpb.Verdict{evalpb.Verdict_VERDICT_PASS, evalpb.Verdict_VERDICT_FAIL, evalpb.Verdict_VERDICT_NOT_APPLICABLE}
)

// A Judge evaluates chats with a model.
type Judge struct {
	aiServiceClient aiservicepb.AiServiceClient
	model           string
}

// NewJudge returns a judge evaluating with model.
// Format: providers/{provider}/models/{model}
func NewJudge(aiServiceClient aiservicepb.AiServiceClient, model string) *Judge {
	return &Judge{aiServiceClient: aiServiceClient, model: model}
}

// EvaluateChat evaluates a chat and records the evaluations on it.
func (j *Judge) EvaluateChat(ctx context.Context, chat string, evals ...*evalpb.Eval) (*aipb.Chat, error) {
	evaluations, err := j.Evaluate(ctx, chat, evals...)
	if err != nil {
		return nil, err
	}
	evaluatedChat := &aipb.Chat{Name: chat}
	if err := Record(evaluatedChat, evaluations...); err != nil {
		return nil, err
	}
	updateChatRequest := &aiservicepb.UpdateChatRequest{
		Chat:       evaluatedChat,
		UpdateMask: UpdateMask(evaluations...).Proto(),
	}
	updatedChat, err := j.aiServiceClient.UpdateChat(ctx, updateChatRequest)
	if err != nil {
		return nil, status.FromError(err, "recording evaluations").Err()
	}
	return updatedChat, nil
}

// Evaluate judges a chat against evals in a single generation and returns one evaluation
// per eval, in order. It records nothing on the chat: see Record. The judge works in a
// chat of its own under the same user, titled after the judged chat.
func (j *Judge) Evaluate(ctx context.Context, chat string, evals ...*evalpb.Eval) ([]*evalpb.Evaluation, error) {
	if err := checkEvals(evals); err != nil {
		return nil, err
	}
	chatRn, err := aipb.ParseChatRn(chat)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing chat: %v", err).Err()
	}

	listMessagesRequest := &aiservicepb.ListMessagesRequest{
		Parent: chat,
		// The conversation as the assistant saw it: failed and superseded turns excluded.
		Filter:  fmt.Sprintf("-status:* AND -labels.%q:*", aipb.Labels.Superseded.GetKey()),
		OrderBy: "create_time asc",
	}
	messages, err := aip.Paginate[*aipb.Message](ctx, listMessagesRequest, j.aiServiceClient.ListMessages)
	if err != nil {
		return nil, status.FromError(err, "listing messages").Err()
	}
	transcript, err := renderTranscript(messages)
	if err != nil {
		return nil, err
	}
	if transcript == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "chat %s has no conversation to evaluate", chat).Err()
	}

	createChatRequest := &aiservicepb.CreateChatRequest{
		Parent: chatRn.UserRn().String(),
		Chat:   &aipb.Chat{Title: chat},
	}
	judgeChat, err := j.aiServiceClient.CreateChat(ctx, createChatRequest)
	if err != nil {
		return nil, status.FromError(err, "creating judge chat").Err()
	}
	generateMessageRequest := &aiservicepb.GenerateMessageRequest{
		Parent: judgeChat.GetName(),
		Model:  j.model,
		Messages: []*aipb.Message{
			ai.NewSystemMessage(ai.NewTextBlock(instructions)),
			ai.NewUserMessage(ai.NewTextBlock(renderCriteria(evals) + "\n\n<transcript>\n" + transcript + "\n</transcript>")),
		},
		Tools: []*aipb.Tool{newVerdictTool(evals)},
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			ToolChoice: &aipb.ToolChoice{Choice: &aipb.ToolChoice_ToolName{ToolName: verdictToolName}},
		},
	}
	generateMessageResponse, err := j.aiServiceClient.GenerateMessage(ctx, generateMessageRequest)
	if err != nil {
		return nil, status.FromError(err, "judging").Err()
	}
	return j.parseVerdicts(generateMessageResponse.GetGeneratedMessage(), evals)
}

// checkEvals rejects an empty or ambiguous set of evals.
func checkEvals(evals []*evalpb.Eval) error {
	if len(evals) == 0 {
		return status.Errorf(codes.InvalidArgument, "no eval to evaluate").Err()
	}
	evalIDSet := make(map[string]struct{}, len(evals))
	for _, eval := range evals {
		if _, ok := evalIDSet[eval.GetId()]; ok {
			return status.Errorf(codes.InvalidArgument, "eval %q given more than once", eval.GetId()).Err()
		}
		evalIDSet[eval.GetId()] = struct{}{}
	}
	return nil
}

func renderCriteria(evals []*evalpb.Eval) string {
	var builder strings.Builder
	builder.WriteString("<criteria>\n")
	for _, eval := range evals {
		fmt.Fprintf(&builder, "- %s: %s\n", eval.GetId(), eval.GetCriterion())
	}
	builder.WriteString("</criteria>")
	return builder.String()
}

// newVerdictTool asks for a verdict on every eval: one required property each, so the
// judge can neither skip nor invent one.
func newVerdictTool(evals []*evalpb.Eval) *aipb.Tool {
	verdictValues, err := verdictMapping.SliceToExternal(verdicts)
	if err != nil {
		panic(err)
	}
	evalIDToSchema := make(map[string]*jsonpb.Schema, len(evals))
	evalIDs := make([]string, 0, len(evals))
	for _, eval := range evals {
		evalIDs = append(evalIDs, eval.GetId())
		evalIDToSchema[eval.GetId()] = &jsonpb.Schema{
			Type:        "object",
			Description: eval.GetCriterion(),
			Properties: map[string]*jsonpb.Schema{
				"rationale": {Type: "string", Description: "Why, citing the transcript. Written before the verdict."},
				"verdict":   {Type: "string", Enum: verdictValues},
			},
			Required: []string{"rationale", "verdict"},
		}
	}
	return &aipb.Tool{
		Name:        verdictToolName,
		Description: "Record a verdict on every criterion.",
		JsonSchema: &jsonpb.Schema{
			Type:       "object",
			Properties: evalIDToSchema,
			Required:   evalIDs,
		},
	}
}

func (j *Judge) parseVerdicts(message *aipb.Message, evals []*evalpb.Eval) ([]*evalpb.Evaluation, error) {
	var arguments *structpb.Struct
	for _, block := range message.GetBlocks() {
		if toolCall := block.GetToolCall(); toolCall.GetName() == verdictToolName {
			arguments = toolCall.GetArguments()
			break
		}
	}
	if arguments == nil {
		return nil, status.Errorf(codes.Internal, "judge did not call %s", verdictToolName).Err()
	}
	evaluateTime := timestamppb.Now()
	evaluations := make([]*evalpb.Evaluation, 0, len(evals))
	for _, eval := range evals {
		fields := arguments.GetFields()[eval.GetId()].GetStructValue().GetFields()
		verdict, err := verdictMapping.FromExternal(fields["verdict"].GetStringValue())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "judge's verdict on %q: %v", eval.GetId(), err).Err()
		}
		evaluations = append(evaluations, &evalpb.Evaluation{
			Eval:         eval.GetId(),
			Verdict:      verdict,
			Rationale:    fields["rationale"].GetStringValue(),
			Model:        j.model,
			EvaluateTime: evaluateTime,
		})
	}
	return evaluations, nil
}
