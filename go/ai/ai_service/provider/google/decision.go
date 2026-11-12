package google

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
)

// Decisions are served by the Gemini Developer API only, and by no released version of the genai SDK yet.
const decisionsURL = "https://generativelanguage.googleapis.com/v1beta/decisions"

// The wire format of POST /v1beta/decisions.
// See: https://ai.google.dev/gemini-api/docs/decisions-api
type decisionRequest struct {
	Model     string                             `json:"model"`
	Input     []decisionRequestContent           `json:"input"`
	Questions map[string]decisionRequestQuestion `json:"questions"`
}

type decisionRequestContent struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Data     []byte `json:"data,omitempty"`
	URI      string `json:"uri,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

type decisionRequestQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	// An object of true and false descriptions, a map of option id to description, or a list of levels.
	Criteria any `json:"criteria,omitempty"`
}

type decisionRequestBooleanCriteria struct {
	TrueDescription  string `json:"true_description,omitempty"`
	FalseDescription string `json:"false_description,omitempty"`
}

type decisionResponse struct {
	Answers map[string]decisionResponseAnswer `json:"answers"`
	Usage   struct {
		TotalInputTokens      int64 `json:"total_input_tokens"`
		InputTokensByModality []struct {
			Modality string `json:"modality"`
			Tokens   int64  `json:"tokens"`
		} `json:"input_tokens_by_modality"`
	} `json:"usage"`
}

type decisionResponseAnswer struct {
	Type   string  `json:"type"`
	Noul   float64 `json:"noul"`
	Choice string  `json:"choice"`
	Score  float64 `json:"score"`
	// Keyed by option id on a choice answer, by level index on a score answer.
	Probabilities map[string]float64 `json:"probabilities"`
}

// CreateDecision implements provider.CreateDecisionClient.
func (c *Client) CreateDecision(ctx context.Context, model *aipb.Model, decision *aipb.Decision) ([]*aipb.Answer, *aipb.ModelUsage, error) {
	request, err := newDecisionRequest(model, decision)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	headers := map[string]string{"x-goog-api-key": c.config.APIKey}
	response := &decisionResponse{}
	if err := provider.PostJSON(ctx, decisionsURL, headers, request, response); err != nil {
		return nil, nil, err
	}
	answers, err := response.answers(decision.GetQuestions())
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return answers, response.modelUsage(), nil
}

func newDecisionRequest(model *aipb.Model, decision *aipb.Decision) (*decisionRequest, error) {
	request := &decisionRequest{
		Model:     model.GetProviderModelId(),
		Questions: make(map[string]decisionRequestQuestion, len(decision.GetQuestions())),
	}
	for i, block := range decision.GetInput() {
		switch content := block.GetContent().(type) {
		case *aipb.Block_Text:
			text := content.Text
			// Gemini joins adjacent text blocks with no separator.
			if i > 0 && decision.GetInput()[i-1].GetText() != "" {
				text = "\n" + text
			}
			request.Input = append(request.Input, decisionRequestContent{Type: "text", Text: text})
		case *aipb.Block_Image:
			request.Input = append(request.Input, decisionRequestContent{
				Type:     "image",
				Data:     content.Image.GetData(),
				URI:      content.Image.GetUrl(),
				MimeType: content.Image.GetMediaType(),
			})
		case *aipb.Block_Document:
			request.Input = append(request.Input, decisionRequestContent{
				Type:     "document",
				Data:     content.Document.GetData(),
				URI:      content.Document.GetUrl(),
				MimeType: content.Document.GetMediaType(),
			})
		default:
			return nil, fmt.Errorf("input [%d]: unsupported block type %T", i, content)
		}
	}
	for _, question := range decision.GetQuestions() {
		requestQuestion := decisionRequestQuestion{Instructions: question.GetInstructions()}
		switch kind := question.GetKind().(type) {
		case *aipb.Question_Boolean:
			requestQuestion.Type = "noul"
			criteria := decisionRequestBooleanCriteria{
				TrueDescription:  kind.Boolean.GetTrueDescription(),
				FalseDescription: kind.Boolean.GetFalseDescription(),
			}
			if criteria != (decisionRequestBooleanCriteria{}) {
				requestQuestion.Criteria = criteria
			}
		case *aipb.Question_Choice:
			requestQuestion.Type, requestQuestion.Criteria = "choice", kind.Choice.GetOptions()
		case *aipb.Question_Score:
			requestQuestion.Type, requestQuestion.Criteria = "score", kind.Score.GetLevels()
		default:
			return nil, fmt.Errorf("question %q: unsupported kind %T", question.GetId(), kind)
		}
		request.Questions[question.GetId()] = requestQuestion
	}
	return request, nil
}

// answers converts the response's answers, keyed by question id, into the order of the questions.
func (r *decisionResponse) answers(questions []*aipb.Question) ([]*aipb.Answer, error) {
	answers := make([]*aipb.Answer, 0, len(questions))
	for _, question := range questions {
		responseAnswer, ok := r.Answers[question.GetId()]
		if !ok {
			return nil, fmt.Errorf("question %q: no answer", question.GetId())
		}
		answer := &aipb.Answer{}
		switch responseAnswer.Type {
		case "noul":
			answer.Kind = &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: responseAnswer.Noul}}
		case "choice":
			answer.Kind = &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{Option: responseAnswer.Choice, Probabilities: responseAnswer.Probabilities}}
		case "score":
			probabilities := make([]float64, len(question.GetScore().GetLevels()))
			for level := range probabilities {
				probabilities[level] = responseAnswer.Probabilities[strconv.Itoa(level)]
			}
			answer.Kind = &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: responseAnswer.Score, Probabilities: probabilities}}
		default:
			return nil, fmt.Errorf("question %q: unknown answer type %q", question.GetId(), responseAnswer.Type)
		}
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *decisionResponse) modelUsage() *aipb.ModelUsage {
	// Video frames and document pages are counted as image tokens; everything else is text or audio.
	var imageTokens int64
	for _, modalityTokens := range r.Usage.InputTokensByModality {
		if modalityTokens.Modality == "image" {
			imageTokens += modalityTokens.Tokens
		}
	}
	return &aipb.ModelUsage{
		InputToken:      ai.NewResourceConsumption(r.Usage.TotalInputTokens - imageTokens),
		InputImageToken: ai.NewResourceConsumption(imageTokens),
	}
}
