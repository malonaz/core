package typesafe

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

const maxRetries = 5

// requestBody is the wire shape of a POST to /v1/systemone.
type requestBody struct {
	State     any                    `json:"state"`
	Model     string                 `json:"model"`
	Questions map[string]questionDTO `json:"questions"`
}

type questionDTO struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions,omitempty"`
	Criteria     any    `json:"criteria,omitempty"`
}

type responseBody struct {
	Model   string               `json:"model"`
	Answers map[string]answerDTO `json:"answers"`
	Usage   struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

type answerDTO struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice,omitempty"`
	Score         float64            `json:"score,omitempty"`
	Noul          float64            `json:"noul,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Confidence    *float64           `json:"confidence,omitempty"`
}

// GenerateDecision implements provider.GenerateDecisionClient.
func (c *Client) GenerateDecision(ctx context.Context, request *aiservicepb.GenerateDecisionRequest) (*aiservicepb.GenerateDecisionResponse, error) {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.GetModel()}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, err
	}

	state, err := stateToDTO(request.GetState())
	if err != nil {
		return nil, err
	}
	body := &requestBody{
		State:     state,
		Model:     model.ProviderModelId,
		Questions: make(map[string]questionDTO, len(request.GetQuestions())),
	}
	for id, question := range request.GetQuestions() {
		dto, err := questionToDTO(question)
		if err != nil {
			return nil, err
		}
		body.Questions[id] = dto
	}

	response, err := c.post(ctx, body)
	if err != nil {
		return nil, err
	}

	answers := make(map[string]*aiservicepb.Answer, len(response.Answers))
	for id, dto := range response.Answers {
		if answers[id], err = dto.toAnswer(); err != nil {
			return nil, err
		}
	}

	modelUsage := &aipb.ModelUsage{
		Model:      request.GetModel(),
		InputToken: &aipb.ResourceConsumption{Quantity: response.Usage.InputTokens},
	}
	if response.Usage.OutputTokens > 0 {
		modelUsage.OutputToken = &aipb.ResourceConsumption{Quantity: response.Usage.OutputTokens}
	}

	return &aiservicepb.GenerateDecisionResponse{Answers: answers, ModelUsage: modelUsage}, nil
}

// post sends the request, retrying on 429/529 with exponential backoff.
func (c *Client) post(ctx context.Context, body *requestBody) (*responseBody, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling jev request: %v", err).Err()
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(payload))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "building jev request: %v", err).Err()
		}
		httpRequest.Header.Set("Authorization", "Bearer "+c.apiKey)
		httpRequest.Header.Set("Content-Type", "application/json")

		httpResponse, err := c.httpClient.Do(httpRequest)
		if err != nil {
			lastErr = status.Errorf(codes.Unavailable, "calling jev: %v", err).Err()
			continue
		}
		responseBytes, err := io.ReadAll(httpResponse.Body)
		httpResponse.Body.Close()
		if err != nil {
			lastErr = status.Errorf(codes.Internal, "reading jev response: %v", err).Err()
			continue
		}

		if httpResponse.StatusCode == http.StatusTooManyRequests || httpResponse.StatusCode == 529 {
			lastErr = status.Errorf(codes.ResourceExhausted, "jev overloaded: %s", string(responseBytes)).Err()
			continue
		}
		if httpResponse.StatusCode != http.StatusOK {
			return nil, status.Errorf(codes.Internal, "jev returned %d: %s", httpResponse.StatusCode, string(responseBytes)).Err()
		}

		var parsed responseBody
		if err := json.Unmarshal(responseBytes, &parsed); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshaling jev response: %v", err).Err()
		}
		return &parsed, nil
	}
	return nil, lastErr
}

// stateToDTO sends a single text block as is and several as an array. Jev only accepts text.
func stateToDTO(blocks []*aipb.Block) (any, error) {
	texts := make([]any, 0, len(blocks))
	for _, block := range blocks {
		text, ok := block.GetContent().(*aipb.Block_Text)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "jev does not support %T state blocks", block.GetContent()).Err()
		}
		texts = append(texts, text.Text)
	}
	if len(texts) == 1 {
		return texts[0], nil
	}
	return texts, nil
}

func questionToDTO(question *aiservicepb.Question) (questionDTO, error) {
	dto := questionDTO{Instructions: question.GetInstructions()}
	switch questionType := question.GetType().(type) {
	case *aiservicepb.Question_Choice:
		criteria := make(map[string]any, len(questionType.Choice.GetCriteria()))
		for key, description := range questionType.Choice.GetCriteria() {
			// Jev takes null for an option without a description.
			criteria[key] = nil
			if description != "" {
				criteria[key] = description
			}
		}
		dto.Type, dto.Criteria = "choice", criteria

	case *aiservicepb.Question_Score:
		dto.Type, dto.Criteria = "score", questionType.Score.GetCriteria()

	case *aiservicepb.Question_Boolean:
		criteria := map[string]string{}
		if criteriaTrue := questionType.Boolean.GetCriteriaTrue(); criteriaTrue != "" {
			criteria["true"] = criteriaTrue
		}
		if criteriaFalse := questionType.Boolean.GetCriteriaFalse(); criteriaFalse != "" {
			criteria["false"] = criteriaFalse
		}
		dto.Type = "noul"
		if len(criteria) > 0 {
			dto.Criteria = criteria
		}

	default:
		return questionDTO{}, status.Errorf(codes.InvalidArgument, "jev does not support %T questions", questionType).Err()
	}
	return dto, nil
}

func (dto answerDTO) toAnswer() (*aiservicepb.Answer, error) {
	answer := &aiservicepb.Answer{}
	switch dto.Type {
	case "choice":
		answer.Type = &aiservicepb.Answer_Choice{Choice: &aiservicepb.ChoiceAnswer{Choice: dto.Choice, Probabilities: dto.Probabilities}}
	case "score":
		answer.Type = &aiservicepb.Answer_Score{Score: &aiservicepb.ScoreAnswer{Score: dto.Score, Probabilities: dto.Probabilities}}
	case "noul":
		answer.Type = &aiservicepb.Answer_Boolean{Boolean: &aiservicepb.BooleanAnswer{Probability: dto.Noul}}
	default:
		return nil, status.Errorf(codes.Internal, "unknown jev answer type %q", dto.Type).Err()
	}
	if dto.Confidence != nil {
		answer.ExtraFields = &structpb.Struct{Fields: map[string]*structpb.Value{"confidence": structpb.NewNumberValue(*dto.Confidence)}}
	}
	return answer, nil
}
