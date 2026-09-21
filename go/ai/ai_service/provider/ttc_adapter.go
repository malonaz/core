package provider

// Adapts any tool-call-capable TTT model into a classifier: forces a single
// structured tool call shaped by the requested questions and parses the
// result back into typed answers.
//
// Unlike a native classifier (e.g. TypeSafe), a TTT model cannot report calibrated
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

// ttcAnswerToolName is the name of the single tool forced on the wrapped model.
const ttcAnswerToolName = "answer_questions"

// ttcAdapter wraps a GenerateMessageClient so it satisfies ClassificationClient.
type ttcAdapter struct {
	client GenerateMessageClient
	model  *aipb.Model
}

// newTTCAdapter wraps client, whose model must have ttt.tool_call set.
func newTTCAdapter(client GenerateMessageClient, model *aipb.Model) ClassificationClient {
	return &ttcAdapter{client: client, model: model}
}

// ProviderId implements the Provider interface.
func (a *ttcAdapter) ProviderId() string { return a.client.ProviderId() }

// Start implements the Provider interface.
func (a *ttcAdapter) Start(ctx context.Context) error { return a.client.Start(ctx) }

// Stop implements the Provider interface.
func (a *ttcAdapter) Stop() { a.client.Stop() }

// Classify implements ClassificationClient by forcing the wrapped model to
// call a single tool shaped by the request's questions.
func (a *ttcAdapter) Classify(ctx context.Context, request *aiservicepb.ClassifyRequest) (*aiservicepb.ClassifyResponse, error) {
	tool, err := ttcBuildTool(request.GetQuestions())
	if err != nil {
		return nil, err
	}

	generateRequest := &aiservicepb.GenerateMessageRequest{
		Model: request.GetModel(),
		Tools: []*aipb.Tool{tool},
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			MaxTokens: a.model.GetTtt().GetOutputTokenLimit(),
			ToolChoice: &aipb.ToolChoice{
				Choice: &aipb.ToolChoice_ToolName{ToolName: ttcAnswerToolName},
			},
		},
	}
	messages := []*aipb.Message{ai.NewUserMessage(ai.NewTextBlock(ttcValueToText(request.GetState())))}

	stream := &ttcCollectorStream{ctx: ctx}
	sender := NewAsyncMessageContentSender(stream, 16)
	generationError := a.client.StreamGenerateMessage(ctx, generateRequest, messages, sender)
	sender.Close()
	if generationError == nil {
		generationError = sender.Wait(ctx)
	}
	if generationError != nil {
		return nil, generationError
	}

	toolCall, modelUsage := stream.result(ttcAnswerToolName)
	if toolCall == nil {
		return nil, status.Errorf(codes.Internal, "model %s did not answer with the forced tool call", request.GetModel()).Err()
	}

	answers, err := ttcBuildAnswers(request.GetQuestions(), toolCall.GetArguments().AsMap())
	if err != nil {
		return nil, err
	}
	return &aiservicepb.ClassifyResponse{Answers: answers, ModelUsage: modelUsage}, nil
}

// ttcCollectorStream is a minimal MessageStream that records every response
// sent by the provider, instead of forwarding it anywhere.
type ttcCollectorStream struct {
	ctx       context.Context
	responses []*aiservicepb.StreamGenerateMessageResponse
}

func (s *ttcCollectorStream) Send(response *aiservicepb.StreamGenerateMessageResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *ttcCollectorStream) Context() context.Context { return s.ctx }

// result scans the recorded responses for the forced tool call and the final
// model usage.
func (s *ttcCollectorStream) result(toolName string) (*aipb.ToolCall, *aipb.ModelUsage) {
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

// ttcBuildTool builds the single tool whose schema has one property per
// question id, forced via ToolChoice so the model must answer all of them.
func ttcBuildTool(questions map[string]*aiservicepb.Question) (*aipb.Tool, error) {
	properties := make(map[string]*jsonpb.Schema, len(questions))
	required := make([]string, 0, len(questions))
	for id, question := range questions {
		schema, err := ttcQuestionSchema(question)
		if err != nil {
			return nil, err
		}
		properties[id] = schema
		required = append(required, id)
	}
	sort.Strings(required)

	return &aipb.Tool{
		Name:        ttcAnswerToolName,
		Description: "Answer every question about the given state.",
		JsonSchema: &jsonpb.Schema{
			Type:       "object",
			Properties: properties,
			Required:   required,
		},
	}, nil
}

func ttcQuestionSchema(question *aiservicepb.Question) (*jsonpb.Schema, error) {
	switch questionType := question.GetType().(type) {
	case *aiservicepb.Question_Choice:
		choice := questionType.Choice
		names := make([]string, 0, len(choice.GetCriteria()))
		for name := range choice.GetCriteria() {
			names = append(names, name)
		}
		sort.Strings(names)
		description := ttcValueToText(choice.GetInstructions())
		for _, name := range names {
			description += fmt.Sprintf("\n- %s: %s", name, ttcValueToText(choice.GetCriteria()[name]))
		}
		return &jsonpb.Schema{Type: "string", Description: description, Enum: names}, nil

	case *aiservicepb.Question_Score:
		score := questionType.Score
		levels := score.GetCriteria()
		description := ttcValueToText(score.GetInstructions())
		for i, level := range levels {
			description += fmt.Sprintf("\n- %d: %s", i, ttcValueToText(level))
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
		description := ttcValueToText(noul.GetInstructions())
		if noul.GetCriteriaTrue() != nil || noul.GetCriteriaFalse() != nil {
			description += fmt.Sprintf("\ntrue: %s\nfalse: %s", ttcValueToText(noul.GetCriteriaTrue()), ttcValueToText(noul.GetCriteriaFalse()))
		}
		return &jsonpb.Schema{Type: "boolean", Description: description}, nil

	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown question type %T", questionType).Err()
	}
}

// ttcBuildAnswers converts the forced tool call's parsed arguments into typed
// Answers. probabilities/confidence are always left unset: a TTT model
// cannot report a calibrated distribution.
func ttcBuildAnswers(questions map[string]*aiservicepb.Question, arguments map[string]any) (map[string]*aiservicepb.Answer, error) {
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
			index, ok := ttcNumberToInt(value)
			if !ok {
				return nil, status.Errorf(codes.Internal, "question %q: expected number, got %T", id, value).Err()
			}
			legend := make(map[string]string, len(levels))
			for i, level := range levels {
				legend[strconv.Itoa(i)] = ttcValueToText(level)
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

func ttcNumberToInt(value any) (int, bool) {
	f, ok := value.(float64)
	if !ok {
		return 0, false
	}
	return int(f), true
}

// ttcValueToText renders a google.protobuf.Value the way it would appear in
// a prompt: a plain string as itself, anything else as JSON.
func ttcValueToText(value interface{ AsInterface() any }) string {
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
