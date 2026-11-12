package typesafe

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
			Id:           "is_spam",
			Instructions: "Is this message spam?",
			Kind: &aipb.Question_Boolean{Boolean: &aipb.BooleanQuestion{
				TrueDescription: "The message is unsolicited advertising.",
			}},
		},
		{
			Id:           "tone",
			Instructions: "What is the tone of this message?",
			Kind: &aipb.Question_Choice{Choice: &aipb.ChoiceQuestion{Options: map[string]string{
				"angry": "An upset or hostile message",
				"calm":  "",
			}}},
		},
		{
			Id:           "urgency",
			Instructions: "How urgent is this message?",
			Kind:         &aipb.Question_Score{Score: &aipb.ScoreQuestion{Levels: []string{"Can wait", "Needs attention this week", "Needs attention today"}}},
		},
	}
}

func TestNewSystemOneRequest(t *testing.T) {
	model := &aipb.Model{ProviderModelId: "jev-1.14.0"}
	decision := &aipb.Decision{
		Input:     []*aipb.Block{{Content: &aipb.Block_Text{Text: "I was charged twice. Please help."}}},
		Questions: testQuestions(),
	}

	request, err := newSystemOneRequest(model, decision)
	require.NoError(t, err)
	requestBytes, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model": "jev-1.14.0",
		"state": "I was charged twice. Please help.",
		"questions": {
			"is_spam": {
				"type": "noul",
				"instructions": "Is this message spam?",
				"criteria": {"true": "The message is unsolicited advertising."}
			},
			"tone": {
				"type": "choice",
				"instructions": "What is the tone of this message?",
				"criteria": {"angry": "An upset or hostile message", "calm": null}
			},
			"urgency": {
				"type": "score",
				"instructions": "How urgent is this message?",
				"criteria": ["Can wait", "Needs attention this week", "Needs attention today"]
			}
		}
	}`, string(requestBytes))

	// Several texts are sent as a list.
	decision.Input = append(decision.Input, &aipb.Block{Content: &aipb.Block_Text{Text: "Order #8821."}})
	request, err = newSystemOneRequest(model, decision)
	require.NoError(t, err)
	require.Equal(t, []string{"I was charged twice. Please help.", "Order #8821."}, request.State)

	// Jev reads text only.
	decision.Input = []*aipb.Block{{Content: &aipb.Block_Image{Image: &aipb.Image{Source: &aipb.Image_Url{Url: "https://example.com/receipt.png"}}}}}
	_, err = newSystemOneRequest(model, decision)
	require.Error(t, err)
}

func TestSystemOneResponse(t *testing.T) {
	response := &systemOneResponse{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"model": "jev-1.14.0",
		"answers": {
			"urgency": {
				"type": "score",
				"score": 1.7,
				"confidence": 0.9,
				"legend": {"0": "Can wait", "1": "Needs attention this week", "2": "Needs attention today"},
				"probabilities": {"0": 0.1, "1": 0.1, "2": 0.8}
			},
			"tone": {"type": "choice", "choice": "angry", "confidence": 0.6, "probabilities": {"angry": 0.8, "calm": 0.2}},
			"is_spam": {"type": "noul", "noul": 0.02}
		},
		"usage": {"input_tokens": 120, "output_tokens": 12}
	}`), response))

	// Answers come back in question order, whatever the order of the response.
	answers, err := response.answers(testQuestions())
	require.NoError(t, err)
	grpcrequire.Equal(t, &aipb.Decision{Answers: []*aipb.Answer{
		{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.02}}},
		{Kind: &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{
			Option:        "angry",
			Probabilities: map[string]float64{"angry": 0.8, "calm": 0.2},
		}}},
		{Kind: &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: 1.7, Probabilities: []float64{0.1, 0.1, 0.8}}}},
	}}, &aipb.Decision{Answers: answers})

	// Every question must be answered.
	delete(response.Answers, "tone")
	_, err = response.answers(testQuestions())
	require.Error(t, err)
}
