package openai

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestNewDecisionRequest(t *testing.T) {
	model := &aipb.Model{ProviderModelId: "gpt-6-luna"}
	decision := &aipb.Decision{
		Input: []*aipb.Block{
			{Content: &aipb.Block_Text{Text: "Inspect the product in this photo."}},
			{Content: &aipb.Block_Image{Image: &aipb.Image{
				Source:    &aipb.Image_Data{Data: []byte("png")},
				MediaType: "image/png",
				Quality:   aipb.ImageQuality_IMAGE_QUALITY_LOW,
			}}},
		},
		Questions: []*aipb.Question{
			{
				Id:           "visible_damage",
				Instructions: "Does the product have visible damage?",
				Kind: &aipb.Question_Boolean{Boolean: &aipb.BooleanQuestion{
					TrueDescription:  "A crack, tear or dent.",
					FalseDescription: "Shadows, or damage to the packaging.",
				}},
			},
			{
				Id:           "department",
				Instructions: "Which department should handle this complaint?",
				Kind: &aipb.Question_Choice{Choice: &aipb.ChoiceQuestion{Options: map[string]string{
					"shipping": "Delivery and tracking.",
					"billing":  "Payments, invoices, and refunds.",
					"other":    "",
				}}},
			},
			{
				Id:           "severity",
				Instructions: "How severe is this issue?",
				Kind:         &aipb.Question_Score{Score: &aipb.ScoreQuestion{Levels: []string{"Cosmetic", "Workaround available", "Fully blocked"}}},
			},
		},
	}

	request, err := newDecisionRequest(model, decision)
	require.NoError(t, err)
	requestBytes, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model": "gpt-6-luna",
		"input": [{"role": "user", "content": [
			{"type": "input_text", "text": "Inspect the product in this photo."},
			{"type": "input_image", "image_url": "data:image/png;base64,cG5n", "detail": "low"}
		]}],
		"questions": [
			{
				"type": "predicate",
				"name": "visible_damage",
				"instructions": "Does the product have visible damage?\nTrue: A crack, tear or dent.\nFalse: Shadows, or damage to the packaging."
			},
			{
				"type": "choice",
				"name": "department",
				"instructions": "Which department should handle this complaint?",
				"choices": [
					{"value": "billing", "description": "Payments, invoices, and refunds."},
					{"value": "other"},
					{"value": "shipping", "description": "Delivery and tracking."}
				]
			},
			{
				"type": "score",
				"name": "severity",
				"instructions": "How severe is this issue?",
				"levels": [{"label": "Cosmetic"}, {"label": "Workaround available"}, {"label": "Fully blocked"}]
			}
		]
	}`, string(requestBytes))
}

func TestNewDecisionRequestRejectsDocuments(t *testing.T) {
	document := &aipb.Document{Source: &aipb.Document_Url{Url: "https://example.com/contract.pdf"}}
	decision := &aipb.Decision{Input: []*aipb.Block{{Content: &aipb.Block_Document{Document: document}}}}
	_, err := newDecisionRequest(&aipb.Model{}, decision)
	require.Error(t, err)
}

func TestDecisionResponse(t *testing.T) {
	response := &decisionResponse{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"model": "gpt-6-luna",
		"answers": [
			{"type": "predicate", "name": "visible_damage", "probability": 0.92},
			{
				"type": "choice",
				"name": "department",
				"choice": "billing",
				"probabilities": [
					{"value": "billing", "probability": 0.95},
					{"value": "other", "probability": 0.02},
					{"value": "shipping", "probability": 0.03}
				],
				"confidence": 0.93
			},
			{
				"type": "score",
				"name": "severity",
				"score": 1.1,
				"probabilities": [
					{"value": 0, "label": "Cosmetic", "probability": 0.1},
					{"value": 1, "label": "Workaround available", "probability": 0.7},
					{"value": 2, "label": "Fully blocked", "probability": 0.2}
				],
				"confidence": 0.55
			},
			{"type": "refusal", "name": "is_lawful"}
		],
		"usage": {
			"input_tokens": 1500,
			"input_tokens_details": {"cached_tokens": 1024, "cache_write_tokens": 0},
			"output_tokens": 0,
			"output_tokens_details": {"reasoning_tokens": 0},
			"total_tokens": 1500
		}
	}`), response))

	answers, err := response.answers()
	require.NoError(t, err)
	grpcrequire.Equal(t, &aipb.Decision{Answers: []*aipb.Answer{
		{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.92}}},
		{Kind: &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{
			Option:        "billing",
			Probabilities: map[string]float64{"billing": 0.95, "other": 0.02, "shipping": 0.03},
		}}},
		{Kind: &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: 1.1, Probabilities: []float64{0.1, 0.7, 0.2}}}},
		{Kind: &aipb.Answer_Refusal{Refusal: &aipb.Refusal{}}},
	}}, &aipb.Decision{Answers: answers})

	// Cached tokens are counted apart from the input tokens that include them.
	grpcrequire.Equal(t, &aipb.ModelUsage{
		InputToken:          &aipb.ResourceConsumption{Quantity: 476},
		InputTokenCacheRead: &aipb.ResourceConsumption{Quantity: 1024},
	}, response.modelUsage())
}
