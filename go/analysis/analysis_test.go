package analysis

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aippb "github.com/malonaz/core/genproto/aip/v1"
	analysisservicepb "github.com/malonaz/core/genproto/analysis/analysis_service/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const (
	parent        = "organizations/o/users/u"
	decisionModel = "providers/mock/models/mock-1"
	textModel     = "providers/mock/models/text-1"
)

type clients struct{}

// titleInput renders the chat's title alone.
func titleInput(_ context.Context, _ clients, chat *aipb.Chat) ([]*aipb.Block, error) {
	return []*aipb.Block{TextBlock("Title: " + chat.GetTitle())}, nil
}

func boolean(id string) *Analysis[*aipb.Chat] {
	return &Analysis[*aipb.Chat]{ID: id, Description: "d", Instructions: "Is it " + id + "?", Output: Boolean()}
}

// fakeAI serves chats from memory, answers every question true and writes "summary".
type fakeAI struct {
	aiservicepb.AiServiceClient
	mutex      sync.Mutex
	nameToChat map[string]*aipb.Chat
	decisions  []*aipb.Decision
	chats      []string
}

func newFakeAI(chats ...*aipb.Chat) *fakeAI {
	f := &fakeAI{nameToChat: map[string]*aipb.Chat{}}
	for _, chat := range chats {
		f.nameToChat[chat.GetName()] = chat
	}
	return f
}

func (f *fakeAI) GetModel(_ context.Context, request *aiservicepb.GetModelRequest, _ ...grpc.CallOption) (*aipb.Model, error) {
	if request.GetName() == textModel {
		return &aipb.Model{Name: request.GetName(), Ttt: &aipb.TttModelConfig{}}, nil
	}
	return &aipb.Model{Name: request.GetName(), Decision: &aipb.DecisionModelConfig{}}, nil
}

func (f *fakeAI) CreateDecision(_ context.Context, request *aiservicepb.CreateDecisionRequest, _ ...grpc.CallOption) (*aipb.Decision, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	decision := proto.Clone(request.GetDecision()).(*aipb.Decision)
	decision.Name = fmt.Sprintf("%s/decisions/%d", request.GetParent(), len(f.decisions))
	for _, question := range decision.GetQuestions() {
		decision.Answers = append(decision.Answers, &aipb.Answer{QuestionId: question.GetId(), Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.8}}})
	}
	f.decisions = append(f.decisions, decision)
	return decision, nil
}

func (f *fakeAI) CreateChat(_ context.Context, request *aiservicepb.CreateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	name := fmt.Sprintf("%s/chats/judge-%d", request.GetParent(), len(f.chats))
	f.chats = append(f.chats, name)
	return &aipb.Chat{Name: name}, nil
}

func (f *fakeAI) GenerateMessage(_ context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (*aiservicepb.GenerateMessageResponse, error) {
	return &aiservicepb.GenerateMessageResponse{GeneratedMessage: &aipb.Message{Blocks: []*aipb.Block{TextBlock("summary")}}}, nil
}

func (f *fakeAI) resource() Resource[*aipb.Chat, clients] {
	return Resource[*aipb.Chat, clients]{
		Get: func(_ context.Context, _ clients, name string) (*aipb.Chat, error) {
			f.mutex.Lock()
			defer f.mutex.Unlock()
			return proto.Clone(f.nameToChat[name]).(*aipb.Chat), nil
		},
		Update: func(_ context.Context, _ clients, chat *aipb.Chat, updateMask *fieldmaskpb.FieldMask) (*aipb.Chat, error) {
			f.mutex.Lock()
			defer f.mutex.Unlock()
			stored := f.nameToChat[chat.GetName()]
			pbfieldmask.New(updateMask).Update(stored, chat)
			stored.Etag += "+"
			stored.UpdateTime = timestamppb.Now()
			return stored, nil
		},
	}
}

func TestRegisterErrors(t *testing.T) {
	ctx := context.Background()
	view := func(analyses ...*Analysis[*aipb.Chat]) View[*aipb.Chat, clients] {
		return View[*aipb.Chat, clients]{Input: titleInput, Analyses: analyses}
	}
	for _, tc := range []struct {
		name     string
		views    []View[*aipb.Chat, clients]
		expected string
	}{
		{
			name:     "no input",
			views:    []View[*aipb.Chat, clients]{{Analyses: []*Analysis[*aipb.Chat]{boolean("a")}}},
			expected: "ai.malonaz.com/Chat: view has no Input",
		},
		{
			name:     "missing output",
			views:    []View[*aipb.Chat, clients]{view(&Analysis[*aipb.Chat]{ID: "a", Description: "d", Instructions: "i"})},
			expected: `analysis "a": missing Output`,
		},
		{
			name:     "bad id",
			views:    []View[*aipb.Chat, clients]{view(boolean("Not_Valid"))},
			expected: "analysis \"Not_Valid\": validation error:\n - name: value does not match regex pattern `^services/[a-z0-9.-]+/types/[A-Z][A-Za-z0-9]*/analyses/[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$` [string.pattern]",
		},
		{
			name:     "invalid output",
			views:    []View[*aipb.Chat, clients]{view(&Analysis[*aipb.Chat]{ID: "a", Description: "d", Instructions: "i", Output: Choice(map[string]string{"only": ""})})},
			expected: "analysis \"a\": validation error:\n - choice.options: map must be at least 2 entries [map.min_pairs]",
		},
		{
			name:     "duplicate id across views",
			views:    []View[*aipb.Chat, clients]{view(boolean("a")), view(boolean("a"))},
			expected: "services/ai.malonaz.com/types/Chat/analyses/a: id already registered",
		},
		{
			name:     "text on a decision model",
			views:    []View[*aipb.Chat, clients]{view(&Analysis[*aipb.Chat]{ID: "a", Description: "d", Instructions: "i", Output: Text(100), Model: decisionModel})},
			expected: "services/ai.malonaz.com/types/Chat/analyses/a: " + decisionModel + " is not a text model",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry(NewJudge(newFakeAI(), &Config{Parent: parent, DecisionModel: decisionModel, ChatModel: textModel}))
			require.EqualError(t, Register(ctx, registry, newFakeAI().resource(), clients{}, tc.views...), tc.expected)
		})
	}
}

func TestAnalysisProto(t *testing.T) {
	for _, tc := range []struct {
		name     string
		analysis *Analysis[*aipb.Chat]
		expected *analysispb.Analysis
	}{
		{
			name: "choice",
			analysis: &Analysis[*aipb.Chat]{
				ID: "topic", Description: "The topic.", Instructions: "Which?", Model: decisionModel,
				Output: Choice(map[string]string{"a": "A", "b": ""}),
			},
			expected: &analysispb.Analysis{
				Name:         "services/ai.malonaz.com/types/Chat/analyses/topic",
				Description:  "The topic.",
				Instructions: "Which?",
				Output:       &analysispb.Analysis_Choice{Choice: &aipb.ChoiceQuestion{Options: map[string]string{"a": "A", "b": ""}}},
				Model:        decisionModel,
				ResourceType: "ai.malonaz.com/Chat",
				LabelKey:     "analysis.malonaz.com/topic",
			},
		},
		{
			name:     "text sets no label",
			analysis: &Analysis[*aipb.Chat]{ID: "summary", Description: "A summary.", Instructions: "Summarise.", Output: Text(200)},
			expected: &analysispb.Analysis{
				Name:         "services/ai.malonaz.com/types/Chat/analyses/summary",
				Description:  "A summary.",
				Instructions: "Summarise.",
				Output:       &analysispb.Analysis_Text{Text: &analysispb.TextOutput{MaxTokens: 200}},
				ResourceType: "ai.malonaz.com/Chat",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			analysis, err := tc.analysis.Proto()
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.expected, analysis, protocmp.Transform()))
		})
	}
}

func TestResourceJSON(t *testing.T) {
	chat := &aipb.Chat{
		Name:        parent + "/chats/c",
		Title:       "Hello",
		Labels:      map[string]string{"team": "support"},
		Annotations: map[string]string{"a": "b"},
		Etag:        "etag",
		UpdateTime:  timestamppb.Now(),
		CreateTime:  &timestamppb.Timestamp{Seconds: 1},
	}
	block, err := ResourceJSON(chat)
	require.NoError(t, err)
	expected := TextBlock("{\n  \"name\": \"organizations/o/users/u/chats/c\",\n  \"create_time\": \"1970-01-01T00:00:01Z\",\n  \"title\": \"Hello\"\n}")
	require.Empty(t, cmp.Diff(expected, block, protocmp.Transform()))
}

// A view's questions share one decision, a text has its own chat, and current results are reused.
func TestGenerate(t *testing.T) {
	ctx := context.Background()
	chatName := parent + "/chats/c"
	ai := newFakeAI(&aipb.Chat{Name: chatName, Title: "Hello"})
	registry := NewRegistry(NewJudge(ai, &Config{Parent: parent, DecisionModel: decisionModel, ChatModel: textModel}))
	never := boolean("never")
	never.Applies = func(*aipb.Chat) bool { return false }
	summary := &Analysis[*aipb.Chat]{ID: "summary", Description: "d", Instructions: "Summarise.", Output: Text(100)}
	require.NoError(t, Register(ctx, registry, ai.resource(), clients{},
		View[*aipb.Chat, clients]{Input: titleInput, Analyses: []*Analysis[*aipb.Chat]{boolean("short"), boolean("greeting"), never, summary}},
		View[*aipb.Chat, clients]{Input: ResourceInput[*aipb.Chat, clients], Analyses: []*Analysis[*aipb.Chat]{boolean("whole")}},
	))
	server := NewServer(registry)

	request := &analysisservicepb.GenerateResultsRequest{Parent: "services/ai.malonaz.com/types/Chat/analyses/-", Resource: chatName}
	response, err := server.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.GetResults(), 4)

	questionIDs := func() [][]string {
		var all [][]string
		for _, decision := range ai.decisions {
			var ids []string
			for _, question := range decision.GetQuestions() {
				ids = append(ids, question.GetId())
			}
			slices.Sort(ids)
			all = append(all, ids)
		}
		return all
	}
	require.Equal(t, [][]string{{"greeting", "short"}, {"whole"}}, questionIDs())
	require.Equal(t, []string{parent + "/chats/judge-0"}, ai.chats)
	chat := ai.nameToChat[chatName]
	require.Equal(t, map[string]string{
		"analysis.malonaz.com/greeting": "true",
		"analysis.malonaz.com/short":    "true",
		"analysis.malonaz.com/whole":    "true",
	}, chat.GetLabels())
	summaryResult, ok, err := summary.Result(chat)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "summary", summaryResult.GetText())

	// Current results come back without a model call.
	again, err := server.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Len(t, ai.decisions, 2)
	require.Len(t, ai.chats, 1)
	require.Empty(t, cmp.Diff(response, again, protocmp.Transform()))

	// A changed resource is judged again.
	ai.nameToChat[chatName].Title = "Goodbye"
	_, err = server.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Len(t, ai.decisions, 4)
	require.Len(t, ai.chats, 2)
}

// Judge calls go under the resource's parent when the adapter names one.
func TestGenerateParent(t *testing.T) {
	ctx := context.Background()
	chatName := "organizations/o/users/owner/chats/c"
	ai := newFakeAI(&aipb.Chat{Name: chatName, Title: "Hello"})
	registry := NewRegistry(NewJudge(ai, &Config{Parent: parent, DecisionModel: decisionModel}))
	resource := ai.resource()
	resource.Parent = func(*aipb.Chat) string { return "organizations/o/users/owner" }
	require.NoError(t, Register(ctx, registry, resource, clients{}, View[*aipb.Chat, clients]{Input: titleInput, Analyses: []*Analysis[*aipb.Chat]{boolean("short")}}))
	_, err := Generate[*aipb.Chat](ctx, registry, chatName)
	require.NoError(t, err)
	require.Equal(t, "organizations/o/users/owner/decisions/0", ai.decisions[0].GetName())
}

func TestBackfill(t *testing.T) {
	ctx := context.Background()
	var chats []*aipb.Chat
	for i := range 5 {
		chats = append(chats, &aipb.Chat{Name: fmt.Sprintf("%s/chats/%d", parent, i), Title: "Hello"})
	}
	ai := newFakeAI(chats...)
	registry := NewRegistry(NewJudge(ai, &Config{Parent: parent, DecisionModel: decisionModel}))
	require.NoError(t, Register(ctx, registry, ai.resource(), clients{}, View[*aipb.Chat, clients]{Input: titleInput, Analyses: []*Analysis[*aipb.Chat]{boolean("short")}}))

	// Pages of two.
	list := func(_ context.Context, pageToken string) ([]string, string, error) {
		offset := 0
		fmt.Sscan(pageToken, &offset)
		end := min(offset+2, len(chats))
		var names []string
		for _, chat := range chats[offset:end] {
			names = append(names, chat.GetName())
		}
		if end == len(chats) {
			return names, "", nil
		}
		return names, fmt.Sprint(end), nil
	}
	count, err := Backfill[*aipb.Chat](ctx, registry, list, 2)
	require.NoError(t, err)
	require.Equal(t, 5, count)
	for _, chat := range chats {
		require.Equal(t, map[string]string{"analysis.malonaz.com/short": "true"}, ai.nameToChat[chat.GetName()].GetLabels())
	}
}

func TestPromptEtag(t *testing.T) {
	analysis, err := boolean("a").Proto()
	require.NoError(t, err)
	chat := &aipb.Chat{Name: parent + "/chats/c", Title: "Hello"}
	etag := func(analysis *analysispb.Analysis, model string, chat *aipb.Chat) string {
		input, err := ResourceInput(context.Background(), clients{}, chat)
		require.NoError(t, err)
		etag, err := PromptEtag(analysis, model, input)
		require.NoError(t, err)
		return etag
	}
	base := etag(analysis, decisionModel, chat)

	recorded := proto.Clone(chat).(*aipb.Chat)
	recorded.Labels = map[string]string{"analysis.malonaz.com/a": "true"}
	recorded.Annotations = map[string]string{"analysis.malonaz.com/a": "x"}
	recorded.Etag, recorded.UpdateTime = "e", timestamppb.Now()
	retitled := proto.Clone(chat).(*aipb.Chat)
	retitled.Title = "Bye"
	reworded := proto.Clone(analysis).(*analysispb.Analysis)
	reworded.Instructions = "Is it really?"
	score := proto.Clone(analysis).(*analysispb.Analysis)
	score.Output = &analysispb.Analysis_Score{Score: &aipb.ScoreQuestion{Levels: []string{"a", "b"}}}
	described := proto.Clone(analysis).(*analysispb.Analysis)
	described.Description = "Other."

	require.Equal(t, []string{base, "different", "different", "different", "different", base}, []string{
		etag(analysis, decisionModel, recorded),
		sameOr(base, etag(analysis, decisionModel, retitled)),
		sameOr(base, etag(reworded, decisionModel, chat)),
		sameOr(base, etag(analysis, "providers/x/models/y", chat)),
		sameOr(base, etag(score, decisionModel, chat)),
		etag(described, decisionModel, chat),
	})
}

func sameOr(base, etag string) string {
	if etag == base {
		return base
	}
	return "different"
}

func TestRecordedOnly(t *testing.T) {
	updated := func(paths ...string) *aippb.ResourceEvent {
		return &aippb.ResourceEvent{Type: aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}}
	}
	events := []*aippb.ResourceEvent{
		updated("labels.`analysis.malonaz.com/polite`", "annotations.`analysis.malonaz.com/polite`"),
		updated("labels.analysis.malonaz.com/polite"),
		updated("labels.`analysis.malonaz.com/polite`", "title"),
		updated("labels"),
		updated(),
		{Type: aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED},
	}
	var got []bool
	for _, event := range events {
		got = append(got, recordedOnly(event))
	}
	require.Equal(t, []bool{true, true, false, false, false, false}, got)
}
