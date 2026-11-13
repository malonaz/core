package analysis

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/ai/ai_service/provider/typesafe"
	"github.com/malonaz/core/go/ai/analysis/fixtures"
	"github.com/malonaz/core/go/analysis"
)

// providerAI answers decisions straight from a provider, without the AI service.
type providerAI struct {
	aiservicepb.AiServiceClient
	provider provider.CreateDecisionClient
	model    *aipb.Model
	mutex    sync.Mutex
	tokens   int64
	calls    int
}

func (p *providerAI) CreateDecision(ctx context.Context, request *aiservicepb.CreateDecisionRequest, _ ...grpc.CallOption) (*aipb.Decision, error) {
	decision := proto.Clone(request.GetDecision()).(*aipb.Decision)
	answers, usage, err := p.provider.CreateDecision(ctx, p.model, decision)
	if err != nil {
		return nil, err
	}
	for i, answer := range answers {
		answer.QuestionId = decision.GetQuestions()[i].GetId()
	}
	decision.Answers = answers
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.tokens += usage.GetInputToken().GetQuantity()
	p.calls++
	return decision, nil
}

// probabilityOf is the probability the judge gave a label value.
func probabilityOf(answer *aipb.Answer, value string) float64 {
	switch kind := answer.GetKind().(type) {
	case *aipb.Answer_Boolean:
		if value == "true" {
			return kind.Boolean.GetProbability()
		}
		return 1 - kind.Boolean.GetProbability()
	case *aipb.Answer_Choice:
		return kind.Choice.GetProbabilities()[value]
	case *aipb.Answer_Score:
		var level int
		fmt.Sscan(value, &level)
		return kind.Score.GetProbabilities()[level]
	}
	return 0
}

// TestEval evaluates every chat question over the fixtures on a real decision model.
func TestEval(t *testing.T) {
	apiKey := os.Getenv("TYPESAFE_API_KEY")
	if apiKey == "" {
		t.Skip("TYPESAFE_API_KEY is unset: run with `plz test -o build.passunsafeenv:TYPESAFE_API_KEY //go/ai/analysis:eval`")
	}
	ctx := context.Background()
	modelName := "providers/typesafe/models/jev-1.14"
	client := &providerAI{provider: typesafe.NewClient(apiKey), model: &aipb.Model{Name: modelName, ProviderModelId: "jev-1.14.0"}}
	judge := analysis.NewJudge(client, &analysis.Config{Parent: "organizations/o/users/u", DecisionModel: modelName})

	type outcome struct {
		fixture, expected, got string
		probability            float64
		edge                   bool
	}
	idToOutcomes := map[string][]outcome{}
	var mutex sync.Mutex
	var wg sync.WaitGroup
	for _, f := range fixtures.All {
		chat := f.Chat("organizations/o/users/u")
		transcript, err := Transcript(f.Messages)
		require.NoError(t, err)
		metadata, err := analysis.ResourceInput(ctx, Clients{}, chat)
		require.NoError(t, err)
		for _, view := range []struct {
			input    []*aipb.Block
			analyses []*analysis.Analysis[*aipb.Chat]
		}{{transcript, TranscriptView.Analyses}, {metadata, MetadataView.Analyses}} {
			for _, a := range view.analyses {
				expected, ok := f.Expected[a.ID]
				if !ok || (a.Applies != nil && !a.Applies(chat)) {
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					result, err := analysis.Evaluate(ctx, judge, a, view.input)
					require.NoError(t, err, "%s %s", f.ID, a.ID)
					got, _ := analysis.LabelValue(result.GetAnswer())
					mutex.Lock()
					defer mutex.Unlock()
					idToOutcomes[a.ID] = append(idToOutcomes[a.ID], outcome{
						fixture: f.ID, expected: expected, got: got, probability: probabilityOf(result.GetAnswer(), expected), edge: f.Edge[a.ID],
					})
				}()
			}
		}
	}
	wg.Wait()

	ids := make([]string, 0, len(idToOutcomes))
	for id := range idToOutcomes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var clearMisses []string
	for _, id := range ids {
		outcomes := idToOutcomes[id]
		sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].fixture < outcomes[j].fixture })
		correct := 0
		for _, o := range outcomes {
			if o.got == o.expected {
				correct++
				continue
			}
			kind := "clear"
			if o.edge {
				kind = "edge"
			} else {
				clearMisses = append(clearMisses, id+"/"+o.fixture)
			}
			t.Logf("MISS %s %s (%s): expected %s got %s, p(expected)=%.2f", id, o.fixture, kind, o.expected, o.got, o.probability)
		}
		t.Logf("ACCURACY %-18s %d/%d", id, correct, len(outcomes))
		for _, o := range outcomes {
			if o.edge {
				t.Logf("EDGE %s %s: expected %s got %s, p(expected)=%.2f", id, o.fixture, o.expected, o.got, o.probability)
			}
		}
	}
	t.Logf("CALLS %d, input tokens %d, price $%.6f", client.calls, client.tokens, float64(client.tokens)*0.042/1e6)
	require.Empty(t, clearMisses)
}
