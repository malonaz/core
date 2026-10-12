package provider

// Adapts any tool-call-capable TTT model into a decision model: forces a single
// structured tool call shaped by the requested questions and parses the
// result back into typed answers.
//
// Unlike a native decision model (e.g. TypeSafe), a TTT model cannot report calibrated
// probabilities or confidence, so those fields are always left unset on the
// returned answers. Kept in this package (rather than a separate one) to
// avoid an import cycle: it needs GenerateMessageClient and
// AsyncMessageContentSender, both defined here.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

// ttdAnswerToolName is the name of the single tool forced on the wrapped model.
const ttdAnswerToolName = "answer_questions"

// ttdAdapter wraps a GenerateMessageClient so it satisfies DecisionClient.
type ttdAdapter struct {
	client GenerateMessageClient
	model  *aipb.Model
}

// newTTDAdapter wraps client, whose model must have ttt.tool_call set.
func newTTDAdapter(client GenerateMessageClient, model *aipb.Model) DecisionClient {
	return &ttdAdapter{client: client, model: model}
}

// ProviderId implements the Provider interface.
func (a *ttdAdapter) ProviderId() string { return a.client.ProviderId() }

// Start implements the Provider interface.
func (a *ttdAdapter) Start(ctx context.Context) error { return a.client.Start(ctx) }

// Stop implements the Provider interface.
func (a *ttdAdapter) Stop() { a.client.Stop() }

// GetDecision implements DecisionClient by forcing the wrapped model to
// call a single tool shaped by the request's questions.
func (a *ttdAdapter) GetDecision(ctx context.Context, request *aiservicepb.GetDecisionRequest) (*aiservicepb.GetDecisionResponse, error) {
	tool, err := ttdBuildTool(request.GetQuestions())
	if err != nil {
		return nil, err
	}

	generateRequest := &aiservicepb.GenerateMessageRequest{
		Model: request.GetModel(),
		Tools: []*aipb.Tool{tool},
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			MaxTokens: a.model.GetTtt().GetOutputTokenLimit(),
			ToolChoice: &aipb.ToolChoice{
				Choice: &aipb.ToolChoice_ToolName{ToolName: ttdAnswerToolName},
			},
		},
	}
	messages := []*aipb.Message{ai.NewUserMessage(ai.NewTextBlock(ttdValueToText(request.GetState())))}

	stream := &ttdCollectorStream{ctx: ctx}
	sender := NewAsyncMessageContentSender(stream, 16)
	generationError := a.client.StreamGenerateMessage(ctx, generateRequest, messages, sender)
	sender.Close()
	if generationError == nil {
		generationError = sender.Wait(ctx)
	}
	if generationError != nil {
		return nil, generationError
	}

	toolCall, modelUsage := stream.result(ttdAnswerToolName)
	if toolCall == nil {
		return nil, status.Errorf(codes.Internal, "model %s did not answer with the forced tool call", request.GetModel()).Err()
	}

	answers, err := ttdBuildAnswers(request.GetQuestions(), toolCall.GetArguments().AsMap())
	if err != nil {
		return nil, err
	}
	return &aiservicepb.GetDecisionResponse{Answers: answers, ModelUsage: modelUsage}, nil
}

// ttdCollectorStream is a minimal MessageStream that records every response
// sent by the provider, instead of forwarding it anywhere.
type ttdCollectorStream struct {
	ctx       context.Context
	responses []*aiservicepb.StreamGenerateMessageResponse
}

func (s *ttdCollectorStream) Send(response *aiservicepb.StreamGenerateMessageResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *ttdCollectorStream) Context() context.Context { return s.ctx }

// result scans the recorded responses for the forced tool call and the final
// model usage.
func (s *ttdCollectorStream) result(toolName string) (*aipb.ToolCall, *aipb.ModelUsage) {
	var toolCall *aipb.ToolCall
	var modelUsage *aipb.ModelUsage
	for _, response := range s.responses {
		switch content := response.GetContent().(type) {
		case *aiservicepb.StreamGenerateMessageResponse_Block:
			if call := content.Block.GetToolCall(); call != nil && call.GetName() == toolName {
				toolCall = call
			}
		case *aiservicepb.StreamGenerateMessageResponse_ModelUsage:
			modelUsage = content.ModelUsage
		}
	}
	return toolCall, modelUsage
}

// ttdBuildTool builds the single tool whose schema has one property per
// question id, forced via ToolChoice so the model must answer all of them.
func ttdBuildTool(questions map[string]*aiservicepb.Question) (*aipb.Tool, error) {
	properties := make(map[string]*jsonpb.Schema, len(questions))
	required := make([]string, 0, len(questions))
	for id, question := range questions {
		schema, err := ttdQuestionSchema(question)
		if err != nil {
			return nil, err
		}
		properties[id] = schema
		required = append(required, id)
	}
	sort.Strings(required)

	return &aipb.Tool{
		Name:        ttdAnswerToolName,
		Description: "Answer every question about the given state.",
		JsonSchema: &jsonpb.Schema{
			Type:       "object",
			Properties: properties,
			Required:   required,
		},
	}, nil
}

func ttdQuestionSchema(question *aiservicepb.Question) (*jsonpb.Schema, error) {
	switch questionType := question.GetType().(type) {
	case *aiservicepb.Question_Choice:
		choice := questionType.Choice
		names := make([]string, 0, len(choice.GetCriteria()))
		for name := range choice.GetCriteria() {
			names = append(names, name)
		}
		sort.Strings(names)
		description := ttdValueToText(choice.GetInstructions())
		for _, name := range names {
			description += fmt.Sprintf("\n- %s: %s", name, ttdValueToText(choice.GetCriteria()[name]))
		}
		return &jsonpb.Schema{Type: "string", Description: description, Enum: names}, nil

	case *aiservicepb.Question_Score:
		score := questionType.Score
		levels := score.GetCriteria()
		description := ttdValueToText(score.GetInstructions())
		for i, level := range levels {
			description += fmt.Sprintf("\n- %d: %s", i, ttdValueToText(level))
		}
		// Integer, not number: unlike Jev's calibrated fractional score, a tool
		// call can only pick one discrete rubric level.
		return &jsonpb.Schema{
			Type:        "integer",
			Description: description,
			Minimum:     0,
			Maximum:     float64(max(len(levels)-1, 0)),
		}, nil

	case *aiservicepb.Question_Noul:
		noul := questionType.Noul
		description := ttdValueToText(noul.GetInstructions())
		if noul.GetCriteriaTrue() != nil || noul.GetCriteriaFalse() != nil {
			description += fmt.Sprintf("\ntrue: %s\nfalse: %s", ttdValueToText(noul.GetCriteriaTrue()), ttdValueToText(noul.GetCriteriaFalse()))
		}
		return &jsonpb.Schema{Type: "boolean", Description: description}, nil

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown question type %T", questionType).Err()
	}
}

// ttdBuildAnswers converts the forced tool call's parsed arguments into typed
// Answers. probabilities/confidence are always left unset: a TTT model
// cannot report a calibrated distribution.
func ttdBuildAnswers(questions map[string]*aiservicepb.Question, arguments map[string]any) (map[string]*aiservicepb.Answer, error) {
	answers := make(map[string]*aiservicepb.Answer, len(questions))
	for id, question := range questions {
		value, ok := arguments[id]
		if !ok {
			return nil, status.Errorf(codes.Internal, "tool call did not answer question %q", id).Err()
		}
		switch questionType := question.GetType().(type) {
		case *aiservicepb.Question_Choice:
			choice, ok := value.(string)
			if !ok {
				return nil, status.Errorf(codes.Internal, "question %q: expected string, got %T", id, value).Err()
			}
			answers[id] = &aiservicepb.Answer{Type: &aiservicepb.Answer_Choice{Choice: &aiservicepb.ChoiceAnswer{Choice: choice}}}

		case *aiservicepb.Question_Score:
			levels := questionType.Score.GetCriteria()
			index, ok := ttdNumberToInt(value)
			if !ok {
				return nil, status.Errorf(codes.Internal, "question %q: expected number, got %T", id, value).Err()
			}
			legend := make(map[string]string, len(levels))
			for i, level := range levels {
				legend[strconv.Itoa(i)] = ttdValueToText(level)
			}
			answers[id] = &aiservicepb.Answer{Type: &aiservicepb.Answer_Score{Score: &aiservicepb.ScoreAnswer{Score: float64(index), Legend: legend}}}

		case *aiservicepb.Question_Noul:
			yes, ok := value.(bool)
			if !ok {
				return nil, status.Errorf(codes.Internal, "question %q: expected bool, got %T", id, value).Err()
			}
			noul := 0.0
			if yes {
				noul = 1.0
			}
			answers[id] = &aiservicepb.Answer{Type: &aiservicepb.Answer_Noul{Noul: &aiservicepb.NoulAnswer{Noul: noul}}}
		}
	}
	return answers, nil
}

func ttdNumberToInt(value any) (int, bool) {
	f, ok := value.(float64)
	if !ok {
		return 0, false
	}
	return int(f), true
}

// ttdValueToText renders a google.protobuf.Value the way it would appear in
// a prompt: a plain string as itself, anything else as JSON.
func ttdValueToText(value interface{ AsInterface() any }) string {
	if value == nil {
		return ""
	}
	if s, ok := value.AsInterface().(string); ok {
		return s
	}
	bytes, err := json.Marshal(value.AsInterface())
	if err != nil {
		return ""
	}
	return string(bytes)
}
