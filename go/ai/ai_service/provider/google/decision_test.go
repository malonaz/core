package google

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func testQuestions() []*aipb.Question {
	return []*aipb.Question{
		{
			Id:           "is_defect",
			Instructions: "Is the customer reporting a product defect?",
			Kind: &aipb.Question_Boolean{Boolean: &aipb.BooleanQuestion{
				TrueDescription:  "The product itself is broken or faulty.",
				FalseDescription: "Preference, sizing, shipping, or pricing complaint.",
			}},
		},
		{
			Id:           "is_urgent",
			Instructions: "Does the customer need an answer today?",
			Kind:         &aipb.Question_Boolean{Boolean: &aipb.BooleanQuestion{}},
		},
		{
			Id:           "department",
			Instructions: "Which department should handle this request?",
			Kind: &aipb.Question_Choice{Choice: &aipb.ChoiceQuestion{Options: map[string]string{
				"billing":  "Payments, invoices, and refunds.",
				"shipping": "Delivery and tracking.",
				"other":    "",
			}}},
		},
		{
			Id:           "severity",
			Instructions: "How severe is this issue for the user?",
			Kind:         &aipb.Question_Score{Score: &aipb.ScoreQuestion{Levels: []string{"Cosmetic", "Workaround available", "Fully blocked"}}},
		},
	}
}

func TestNewDecisionRequest(t *testing.T) {
	model := &aipb.Model{ProviderModelId: "gemini-decision-1"}
	decision := &aipb.Decision{
		Input: []*aipb.Block{
			{Content: &aipb.Block_Text{Text: "Return request #8821."}},
			{Content: &aipb.Block_Text{Text: "The left earcup cuts out."}},
			{Content: &aipb.Block_Image{Image: &aipb.Image{
				Source:    &aipb.Image_Url{Url: "https://example.com/earcup.jpg"},
				MediaType: "image/jpeg",
			}}},
			{Content: &aipb.Block_Document{Document: &aipb.Document{
				Source:    &aipb.Document_Data{Data: []byte("pdf")},
				MediaType: "application/pdf",
			}}},
		},
		Questions: testQuestions(),
	}

	request, err := newDecisionRequest(model, decision)
	require.NoError(t, err)
	requestBytes, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model": "gemini-decision-1",
		"input": [
			{"type": "text", "text": "Return request #8821."},
			{"type": "text", "text": "\nThe left earcup cuts out."},
			{"type": "image", "uri": "https://example.com/earcup.jpg", "mime_type": "image/jpeg"},
			{"type": "document", "data": "cGRm", "mime_type": "application/pdf"}
		],
		"questions": {
			"is_defect": {
				"type": "noul",
				"instructions": "Is the customer reporting a product defect?",
				"criteria": {
					"true_description": "The product itself is broken or faulty.",
					"false_description": "Preference, sizing, shipping, or pricing complaint."
				}
			},
			"is_urgent": {"type": "noul", "instructions": "Does the customer need an answer today?"},
			"department": {
				"type": "choice",
				"instructions": "Which department should handle this request?",
				"criteria": {"billing": "Payments, invoices, and refunds.", "shipping": "Delivery and tracking.", "other": ""}
			},
			"severity": {
				"type": "score",
				"instructions": "How severe is this issue for the user?",
				"criteria": ["Cosmetic", "Workaround available", "Fully blocked"]
			}
		}
	}`, string(requestBytes))
}

func TestDecisionResponse(t *testing.T) {
	response := &decisionResponse{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"answers": {
			"severity": {
				"type": "score",
				"score": 1.12,
				"legend": {"0": "Cosmetic", "1": "Workaround available", "2": "Fully blocked"},
				"probabilities": {"0": 0.03, "1": 0.82, "2": 0.15},
				"confidence": 0.82
			},
			"department": {
				"type": "choice",
				"choice": "billing",
				"probabilities": {"billing": 0.99, "shipping": 0.004, "other": 0.006},
				"confidence": 0.99
			},
			"is_urgent": {"type": "noul", "noul": 0.94, "confidence": 0.94},
			"is_defect": {"type": "noul", "noul": 0.03, "confidence": 0.97}
		},
		"usage": {
			"total_input_tokens": 424,
			"input_tokens_by_modality": [{"modality": "text", "tokens": 100}, {"modality": "image", "tokens": 324}],
			"total_output_tokens": 0,
			"total_tokens": 424
		}
	}`), response))

	// Answers come back in question order, whatever the order of the response.
	answers, err := response.answers(testQuestions())
	require.NoError(t, err)
	grpcrequire.Equal(t, &aipb.Decision{Answers: []*aipb.Answer{
		{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.03}}},
		{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.94}}},
		{Kind: &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{
			Option:        "billing",
			Probabilities: map[string]float64{"billing": 0.99, "shipping": 0.004, "other": 0.006},
		}}},
		{Kind: &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: 1.12, Probabilities: []float64{0.03, 0.82, 0.15}}}},
	}}, &aipb.Decision{Answers: answers})

	grpcrequire.Equal(t, &aipb.ModelUsage{
		InputToken:      &aipb.ResourceConsumption{Quantity: 100},
		InputImageToken: &aipb.ResourceConsumption{Quantity: 324},
	}, response.modelUsage())

	// Every question must be answered.
	delete(response.Answers, "is_urgent")
	_, err = response.answers(testQuestions())
	require.Error(t, err)
}
