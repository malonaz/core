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
	Instructions any    `json:"instructions"`
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
	Legend        map[string]string  `json:"legend,omitempty"`
	Noul          float64            `json:"noul,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Confidence    *float64           `json:"confidence,omitempty"`
}

// Classify implements provider.ClassificationClient.
func (c *Client) Classify(ctx context.Context, request *aiservicepb.ClassifyRequest) (*aiservicepb.ClassifyResponse, error) {
	body := &requestBody{
		State:     request.GetState().AsInterface(),
		Model:     "jev-latest",
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
		answers[id] = dto.toAnswer()
	}

	modelUsage := &aipb.ModelUsage{
		Model:      request.GetModel(),
		InputToken: &aipb.ResourceConsumption{Quantity: response.Usage.InputTokens},
	}
	if response.Usage.OutputTokens > 0 {
		modelUsage.OutputToken = &aipb.ResourceConsumption{Quantity: response.Usage.OutputTokens}
	}

	return &aiservicepb.ClassifyResponse{Answers: answers, ModelUsage: modelUsage}, nil
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

func questionToDTO(question *aiservicepb.Question) (questionDTO, error) {
	switch questionType := question.GetType().(type) {
	case *aiservicepb.Question_Choice:
		criteria := make(map[string]any, len(questionType.Choice.GetCriteria()))
		for name, value := range questionType.Choice.GetCriteria() {
			criteria[name] = value.AsInterface()
		}
		return questionDTO{
			Type:         "choice",
			Instructions: questionType.Choice.GetInstructions().AsInterface(),
			Criteria:     criteria,
		}, nil

	case *aiservicepb.Question_Score:
		levels := questionType.Score.GetCriteria()
		criteria := make([]any, 0, len(levels))
		for _, level := range levels {
			criteria = append(criteria, level.AsInterface())
		}
		return questionDTO{
			Type:         "score",
			Instructions: questionType.Score.GetInstructions().AsInterface(),
			Criteria:     criteria,
		}, nil

	case *aiservicepb.Question_Noul:
		criteria := map[string]any{
			"true":  questionType.Noul.GetCriteriaTrue().AsInterface(),
			"false": questionType.Noul.GetCriteriaFalse().AsInterface(),
		}
		return questionDTO{
			Type:         "noul",
			Instructions: questionType.Noul.GetInstructions().AsInterface(),
			Criteria:     criteria,
		}, nil

	default:
		return questionDTO{}, status.Errorf(codes.InvalidArgument, "unknown question type %T", questionType).Err()
	}
}

func (dto answerDTO) toAnswer() *aiservicepb.Answer {
	switch dto.Type {
	case "choice":
		return &aiservicepb.Answer{Type: &aiservicepb.Answer_Choice{Choice: &aiservicepb.ChoiceAnswer{
			Choice:        dto.Choice,
			Probabilities: dto.Probabilities,
			Confidence:    dto.Confidence,
		}}}
	case "score":
		return &aiservicepb.Answer{Type: &aiservicepb.Answer_Score{Score: &aiservicepb.ScoreAnswer{
			Score:         dto.Score,
			Legend:        dto.Legend,
			Probabilities: dto.Probabilities,
			Confidence:    dto.Confidence,
		}}}
	case "noul":
		return &aiservicepb.Answer{Type: &aiservicepb.Answer_Noul{Noul: &aiservicepb.NoulAnswer{Noul: dto.Noul}}}
	default:
		return nil
	}
}
