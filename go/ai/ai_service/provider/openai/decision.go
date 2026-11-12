package openai

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"

	"google.golang.org/grpc/codes"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
)

// The wire format of POST /decisions.
// See: https://developers.openai.com/api/reference/resources/decisions
type decisionRequest struct {
	Model     string                    `json:"model"`
	Input     []decisionRequestMessage  `json:"input"`
	Questions []decisionRequestQuestion `json:"questions"`
}

type decisionRequestMessage struct {
	Role    string                `json:"role"`
	Content []decisionRequestPart `json:"content"`
}

type decisionRequestPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

type decisionRequestQuestion struct {
	Type         string                  `json:"type"`
	Name         string                  `json:"name"`
	Instructions string                  `json:"instructions"`
	Choices      []decisionRequestChoice `json:"choices,omitempty"`
	Levels       []decisionRequestLevel  `json:"levels,omitempty"`
}

type decisionRequestChoice struct {
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
}

type decisionRequestLevel struct {
	Label string `json:"label"`
}

type decisionResponse struct {
	Answers []decisionResponseAnswer `json:"answers"`
	Usage   struct {
		InputTokens        int64 `json:"input_tokens"`
		InputTokensDetails struct {
			CachedTokens     int64 `json:"cached_tokens"`
			CacheWriteTokens int64 `json:"cache_write_tokens"`
		} `json:"input_tokens_details"`
	} `json:"usage"`
}

type decisionResponseAnswer struct {
	Type          string  `json:"type"`
	Probability   float64 `json:"probability"`
	Choice        string  `json:"choice"`
	Score         float64 `json:"score"`
	Probabilities []struct {
		// An option id on a choice answer, a level index on a score answer.
		Value       any     `json:"value"`
		Probability float64 `json:"probability"`
	} `json:"probabilities"`
}

// CreateDecision implements provider.CreateDecisionClient.
func (c *Client) CreateDecision(ctx context.Context, model *aipb.Model, decision *aipb.Decision) ([]*aipb.Answer, *aipb.ModelUsage, error) {
	request, err := newDecisionRequest(model, decision)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	headers := map[string]string{"Authorization": "Bearer " + c.apiKey}
	response := &decisionResponse{}
	if err := provider.PostJSON(ctx, c.config.BaseUrl+"/decisions", headers, request, response); err != nil {
		return nil, nil, err
	}
	answers, err := response.answers()
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return answers, response.modelUsage(), nil
}

func newDecisionRequest(model *aipb.Model, decision *aipb.Decision) (*decisionRequest, error) {
	parts := make([]decisionRequestPart, 0, len(decision.GetInput()))
	for i, block := range decision.GetInput() {
		switch content := block.GetContent().(type) {
		case *aipb.Block_Text:
			parts = append(parts, decisionRequestPart{Type: "input_text", Text: content.Text})
		case *aipb.Block_Image:
			url := content.Image.GetUrl()
			if data := content.Image.GetData(); data != nil {
				url = fmt.Sprintf("data:%s;base64,%s", content.Image.GetMediaType(), base64.StdEncoding.EncodeToString(data))
			}
			parts = append(parts, decisionRequestPart{Type: "input_image", ImageURL: url, Detail: imageQualityToOpenAI[content.Image.GetQuality()]})
		default:
			return nil, fmt.Errorf("input [%d]: unsupported block type %T", i, content)
		}
	}

	questions := make([]decisionRequestQuestion, 0, len(decision.GetQuestions()))
	for _, question := range decision.GetQuestions() {
		requestQuestion := decisionRequestQuestion{Name: question.GetId(), Instructions: question.GetInstructions()}
		switch kind := question.GetKind().(type) {
		case *aipb.Question_Boolean:
			requestQuestion.Type = "predicate"
			// A predicate takes no criteria: what counts as true or false joins the instructions.
			if description := kind.Boolean.GetTrueDescription(); description != "" {
				requestQuestion.Instructions += "\nTrue: " + description
			}
			if description := kind.Boolean.GetFalseDescription(); description != "" {
				requestQuestion.Instructions += "\nFalse: " + description
			}
		case *aipb.Question_Choice:
			requestQuestion.Type = "choice"
			// Sorted, as the map has no order of its own and the request should be the same every time.
			for _, option := range slices.Sorted(maps.Keys(kind.Choice.GetOptions())) {
				requestQuestion.Choices = append(requestQuestion.Choices, decisionRequestChoice{Value: option, Description: kind.Choice.GetOptions()[option]})
			}
		case *aipb.Question_Score:
			requestQuestion.Type = "score"
			for _, level := range kind.Score.GetLevels() {
				requestQuestion.Levels = append(requestQuestion.Levels, decisionRequestLevel{Label: level})
			}
		default:
			return nil, fmt.Errorf("question %q: unsupported kind %T", question.GetId(), kind)
		}
		questions = append(questions, requestQuestion)
	}

	return &decisionRequest{
		Model:     model.GetProviderModelId(),
		Input:     []decisionRequestMessage{{Role: "user", Content: parts}},
		Questions: questions,
	}, nil
}

// answers converts the response's answers, which come in question order.
func (r *decisionResponse) answers() ([]*aipb.Answer, error) {
	answers := make([]*aipb.Answer, 0, len(r.Answers))
	for i, responseAnswer := range r.Answers {
		answer := &aipb.Answer{}
		switch responseAnswer.Type {
		case "predicate":
			answer.Kind = &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: responseAnswer.Probability}}
		case "choice":
			optionToProbability := make(map[string]float64, len(responseAnswer.Probabilities))
			for _, probability := range responseAnswer.Probabilities {
				option, ok := probability.Value.(string)
				if !ok {
					return nil, fmt.Errorf("answer [%d]: unexpected option %v", i, probability.Value)
				}
				optionToProbability[option] = probability.Probability
			}
			answer.Kind = &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{Option: responseAnswer.Choice, Probabilities: optionToProbability}}
		case "score":
			probabilities := make([]float64, len(responseAnswer.Probabilities))
			for _, probability := range responseAnswer.Probabilities {
				level, ok := probability.Value.(float64)
				if !ok || int(level) < 0 || int(level) >= len(probabilities) {
					return nil, fmt.Errorf("answer [%d]: unexpected level %v", i, probability.Value)
				}
				probabilities[int(level)] = probability.Probability
			}
			answer.Kind = &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: responseAnswer.Score, Probabilities: probabilities}}
		case "refusal":
			answer.Kind = &aipb.Answer_Refusal{Refusal: &aipb.Refusal{}}
		default:
			return nil, fmt.Errorf("answer [%d]: unknown type %q", i, responseAnswer.Type)
		}
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *decisionResponse) modelUsage() *aipb.ModelUsage {
	// input_tokens includes the cached tokens, which ModelUsage counts apart.
	details := r.Usage.InputTokensDetails
	return &aipb.ModelUsage{
		InputToken:           ai.NewResourceConsumption(r.Usage.InputTokens - details.CachedTokens - details.CacheWriteTokens),
		InputTokenCacheRead:  ai.NewResourceConsumption(details.CachedTokens),
		InputTokenCacheWrite: ai.NewResourceConsumption(details.CacheWriteTokens),
	}
}
