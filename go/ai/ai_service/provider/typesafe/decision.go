package typesafe

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

const systemOneURL = "https://api.typesafe.ai/v1/systemone"

// The wire format of POST /v1/systemone.
// See: https://docs.typesafe.ai/api/system-one
type systemOneRequest struct {
	Model string `json:"model"`
	// A text, or a list of texts.
	State     any                          `json:"state"`
	Questions map[string]systemOneQuestion `json:"questions"`
}

type systemOneQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	// A map of "true" and "false" to their description, a map of option id to
	// description (null for none), or a list of levels.
	Criteria any `json:"criteria,omitempty"`
}

type systemOneResponse struct {
	Answers map[string]systemOneAnswer `json:"answers"`
	Usage   struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

type systemOneAnswer struct {
	Type   string  `json:"type"`
	Noul   float64 `json:"noul"`
	Choice string  `json:"choice"`
	Score  float64 `json:"score"`
	// Keyed by option id on a choice answer, by level index on a score answer.
	Probabilities map[string]float64 `json:"probabilities"`
}

// CreateDecision implements provider.CreateDecisionClient.
func (c *Client) CreateDecision(ctx context.Context, model *aipb.Model, decision *aipb.Decision) ([]*aipb.Answer, *aipb.ModelUsage, error) {
	request, err := newSystemOneRequest(model, decision)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	headers := map[string]string{"Authorization": "Bearer " + c.apiKey}
	response := &systemOneResponse{}
	if err := provider.PostJSON(ctx, systemOneURL, headers, request, response); err != nil {
		return nil, nil, err
	}
	answers, err := response.answers(decision.GetQuestions())
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	modelUsage := &aipb.ModelUsage{
		InputToken:  ai.NewResourceConsumption(response.Usage.InputTokens),
		OutputToken: ai.NewResourceConsumption(response.Usage.OutputTokens),
	}
	return answers, modelUsage, nil
}

func newSystemOneRequest(model *aipb.Model, decision *aipb.Decision) (*systemOneRequest, error) {
	texts := make([]string, 0, len(decision.GetInput()))
	for i, block := range decision.GetInput() {
		content, ok := block.GetContent().(*aipb.Block_Text)
		if !ok {
			return nil, fmt.Errorf("input [%d]: unsupported block type %T", i, block.GetContent())
		}
		texts = append(texts, content.Text)
	}
	request := &systemOneRequest{
		Model:     model.GetProviderModelId(),
		State:     texts,
		Questions: make(map[string]systemOneQuestion, len(decision.GetQuestions())),
	}
	if len(texts) == 1 {
		request.State = texts[0]
	}
	for _, question := range decision.GetQuestions() {
		requestQuestion := systemOneQuestion{Instructions: question.GetInstructions()}
		switch kind := question.GetKind().(type) {
		case *aipb.Question_Boolean:
			requestQuestion.Type = "noul"
			criteria := map[string]string{}
			if description := kind.Boolean.GetTrueDescription(); description != "" {
				criteria["true"] = description
			}
			if description := kind.Boolean.GetFalseDescription(); description != "" {
				criteria["false"] = description
			}
			if len(criteria) > 0 {
				requestQuestion.Criteria = criteria
			}
		case *aipb.Question_Choice:
			criteria := make(map[string]*string, len(kind.Choice.GetOptions()))
			for option, description := range kind.Choice.GetOptions() {
				criteria[option] = nil
				if description != "" {
					criteria[option] = &description
				}
			}
			requestQuestion.Type, requestQuestion.Criteria = "choice", criteria
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
func (r *systemOneResponse) answers(questions []*aipb.Question) ([]*aipb.Answer, error) {
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
