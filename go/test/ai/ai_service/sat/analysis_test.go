package sat

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	aippb "github.com/malonaz/core/genproto/aip/v1"
	analysisservicepb "github.com/malonaz/core/genproto/analysis/analysis_service/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	natspb "github.com/malonaz/core/genproto/nats/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider/mock"
	chatanalysis "github.com/malonaz/core/go/ai/analysis"
	"github.com/malonaz/core/go/ai/analysis/fixtures"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/analysis"
	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/uuid"
)

const chatAnalyses = "services/ai.malonaz.com/types/Chat/analyses/"

var (
	analysisOnce     sync.Once
	analysisServer   *analysis.Server
	analysisRegistry *analysis.Registry
	natsClient       *nats.Client
	// Judge decisions are created under this user.
	judgeParent = newUserParent()
	// The AI service emits no chat events, so the tests publish them, shaped as codegen's.
	chatStream = nats.NewStream(&natspb.StreamOptions{Name: "test.analysis.chat"})
	// Each handled event's resource name, once handled.
	handledEvents = make(chan string, 100)
)

// startAnalysis runs the chat analyses' event processor and server in the test process, on the mock model.
func startAnalysis(t *testing.T) {
	t.Helper()
	analysisOnce.Do(func() {
		var err error
		natsClient, err = satEnvironment.GetNatsClient(ctx)
		require.NoError(t, err)
		_, err = natsClient.CreateOrUpdateStream(ctx, chatStream)
		require.NoError(t, err)

		judge := analysis.NewJudge(aiServiceClient, &analysis.Config{Parent: judgeParent, DecisionModel: mockModel, ChatModel: mockModel},
			analysis.WithDecisionHook(scriptDecision), analysis.WithMessageHook(scriptSummary))
		registry := analysis.NewRegistry(judge)
		require.NoError(t, analysis.Register(ctx, registry, chatanalysis.Chat, chatanalysis.Clients{AI: aiServiceClient}, chatanalysis.Views...))
		handle := analysis.HandleEvents[*aipb.Chat](registry)
		processorConfig := &nats.ProcessorConfig{
			Subjects:     []*nats.Subject{chatStream.Subject("created"), chatStream.Subject("updated")},
			ConsumerName: "ai-analysis",
			BatchSize:    10,
			AckWait:      30 * time.Second,
		}
		groupKey := func(message *nats.Message[*aippb.ResourceEvent]) (string, error) {
			return message.Payload.GetName(), nil
		}
		processor := nats.NewProcessor(natsClient, processorConfig, func(ctx context.Context, message *nats.Message[*aippb.ResourceEvent]) error {
			err := handle(ctx, message)
			handledEvents <- message.Payload.GetName()
			return err
		}, nats.WithGroupKey(groupKey))
		require.NoError(t, processor.Start(ctx))
		analysisServer = analysis.NewServer(registry)
		analysisRegistry = registry
	})
}

// publishChatEvent publishes a chat event as codegen would.
func publishChatEvent(t *testing.T, eventType aippb.ResourceEventType, chat *aipb.Chat, paths ...string) {
	t.Helper()
	resource, err := anypb.New(chat)
	require.NoError(t, err)
	event := &aippb.ResourceEvent{Name: chat.GetName(), Type: eventType, Resource: resource}
	subject := "created"
	if eventType == aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED {
		subject = "updated"
		event.PreviousResource, event.UpdateMask = resource, &fieldmaskpb.FieldMask{Paths: paths}
	}
	require.NoError(t, natsClient.Publish(ctx, chatStream.Subject(subject), event))
}

// waitHandled waits until n events of a chat are handled.
func waitHandled(t *testing.T, name string, n int) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for n > 0 {
		select {
		case handled := <-handledEvents:
			if handled == name {
				n--
			} else {
				handledEvents <- handled
			}
		case <-timeout:
			t.Fatalf("%s: events not handled", name)
		}
	}
}

// fixtureOf finds the fixture an input is about.
func fixtureOf(input []*aipb.Block) (*fixtures.Fixture, error) {
	text := input[0].GetText()
	for _, f := range fixtures.All {
		if strings.Contains(text, f.Messages[0].GetBlocks()[0].GetText()) || strings.Contains(text, "/chats/"+f.ID+"-") {
			return f, nil
		}
	}
	return nil, fmt.Errorf("no fixture for input %q", text)
}

// scriptDecision scripts the mock with the expected answers of the fixture the decision is about.
func scriptDecision(decision *aipb.Decision) error {
	f, err := fixtureOf(decision.GetInput())
	if err != nil {
		return err
	}
	answers, err := f.Script(decision)
	if err != nil {
		return err
	}
	script, err := mock.Script(answers...)
	if err != nil {
		return err
	}
	decision.Annotations = map[string]string{aipb.Annotations.Script.Key: script}
	return nil
}

// scriptSummary scripts the mock with the summary of the fixture the message is about.
func scriptSummary(message *aipb.Message) error {
	f, err := fixtureOf(message.GetBlocks())
	if err != nil {
		return err
	}
	script, err := mock.Script(ai.NewAssistantMessage(ai.NewTextBlock(f.Summary())))
	if err != nil {
		return err
	}
	message.Annotations = map[string]string{aipb.Annotations.Script.Key: script}
	return nil
}

// createFixtureChat creates a fixture's chat and messages.
func createFixtureChat(t *testing.T, parent string, f *fixtures.Fixture) (*aipb.Chat, []*aipb.Message) {
	t.Helper()
	chat, err := aiServiceClient.CreateChat(ctx, &aiservicepb.CreateChatRequest{
		Parent: parent,
		ChatId: f.ID + "-" + strings.ReplaceAll(uuid.MustNewV7().String(), "-", "")[:12],
		Chat:   &aipb.Chat{Title: f.Title, Labels: f.Labels},
	})
	require.NoError(t, err)
	var messages []*aipb.Message
	for _, message := range f.Messages {
		created, err := aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{Parent: chat.GetName(), Message: message})
		require.NoError(t, err)
		messages = append(messages, created)
	}
	return chat, messages
}

// expectedLabels are the labels the fixture's analyses should leave.
func expectedLabels(f *fixtures.Fixture) map[string]string {
	labels := map[string]string{}
	for key, value := range f.Labels {
		labels[key] = value
	}
	for id, value := range f.Expected {
		if id == "support-tone" && f.Labels["team"] != "support" {
			continue
		}
		labels[analysispb.Labels.Analysis.GetKey(id)] = value
	}
	return labels
}

// waitForLabels polls the chat until it carries n labels.
func waitForLabels(t *testing.T, name string, n int) *aipb.Chat {
	t.Helper()
	var chat *aipb.Chat
	require.Eventually(t, func() bool {
		var err error
		chat, err = aiServiceClient.GetChat(ctx, &aiservicepb.GetChatRequest{Name: name})
		require.NoError(t, err)
		return len(chat.GetLabels()) >= n
	}, 20*time.Second, 100*time.Millisecond)
	return chat
}

// results decodes the chat's analysis annotations by analysis id.
func results(t *testing.T, chat *aipb.Chat) map[string]*analysispb.Result {
	t.Helper()
	idToResult := map[string]*analysispb.Result{}
	for _, analysis := range chatanalysis.All {
		result, ok, err := analysis.Result(chat)
		require.NoError(t, err)
		if ok {
			idToResult[analysis.ID] = result
		}
	}
	return idToResult
}

// decisionQuestions lists the question ids of each decision the answers came from.
func decisionQuestions(t *testing.T, idToResult map[string]*analysispb.Result) map[string][]string {
	t.Helper()
	sourceToQuestions := map[string][]string{}
	for _, result := range idToResult {
		if _, ok := sourceToQuestions[result.GetSource()]; ok || result.GetAnswer() == nil {
			continue
		}
		decision, err := aiServiceClient.GetDecision(ctx, &aiservicepb.GetDecisionRequest{Name: result.GetSource()})
		require.NoError(t, err)
		for _, question := range decision.GetQuestions() {
			sourceToQuestions[result.GetSource()] = append(sourceToQuestions[result.GetSource()], question.GetId())
		}
	}
	return sourceToQuestions
}

func countDecisions(t *testing.T) int {
	t.Helper()
	decisions, err := aip.Paginate[*aipb.Decision](ctx, &aiservicepb.ListDecisionsRequest{Parent: judgeParent}, aiServiceClient.ListDecisions)
	require.NoError(t, err)
	return len(decisions)
}

// A created event runs every analysis due; analyses sharing an input share a decision.
func TestAnalysisEvents(t *testing.T) {
	startAnalysis(t)
	parent := newUserParent()
	nameToFixture := map[string]*fixtures.Fixture{}
	for _, f := range fixtures.All {
		chat, _ := createFixtureChat(t, parent, f)
		nameToFixture[chat.GetName()] = f
		publishChatEvent(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED, chat)
	}

	for name, f := range nameToFixture {
		labels := expectedLabels(f)
		chat := waitForLabels(t, name, len(labels))
		require.Equal(t, labels, chat.GetLabels(), f.ID)

		idToResult := results(t, chat)
		for id, result := range idToResult {
			expected := &analysispb.Result{
				Name:     analysis.ResultName(chatAnalyses+id, name),
				Resource: name,
				Value:    &analysispb.Result_Text{Text: f.Summary()},
				Model:    mockModel,
			}
			if id != chatanalysis.Summary.ID {
				answer, err := fixtures.Answer(id, f.Expected[id])
				require.NoError(t, err)
				answer.QuestionId = id
				expected.Value = &analysispb.Result_Answer{Answer: answer}
			}
			require.Empty(t, cmp.Diff(expected, result, protocmp.Transform(),
				protocmp.IgnoreFields(&analysispb.Result{}, "prompt_etag", "source", "analyze_time")), f.ID)
		}

		// The transcript questions share one decision; untitled, rendered from the chat, has its own.
		transcript := []string{"caller-intent", "polite", "premature-hang-up", "resolution"}
		if f.Labels["team"] == "support" {
			transcript = append(transcript, "support-tone")
		}
		questions := decisionQuestions(t, idToResult)
		require.ElementsMatch(t, [][]string{transcript, {"untitled"}}, sortedValues(questions), f.ID)
	}

	// Results are filterable labels.
	chats, err := aip.Paginate[*aipb.Chat](ctx, &aiservicepb.ListChatsRequest{Parent: parent, Filter: `labels."analysis.malonaz.com/polite" = "false"`}, aiServiceClient.ListChats)
	require.NoError(t, err)
	var impolite []string
	for _, chat := range chats {
		impolite = append(impolite, nameToFixture[chat.GetName()].ID)
	}
	require.Equal(t, []string{"rude-complaint"}, impolite)
}

func sortedValues(m map[string][]string) [][]string {
	var values [][]string
	for _, ids := range m {
		values = append(values, sortedCopy(ids))
	}
	return values
}

func sortedCopy(ids []string) []string {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}

// GenerateResults returns current results without a model call, and judges stale ones again.
func TestAnalysisGenerateResults(t *testing.T) {
	startAnalysis(t)
	parent := newUserParent()
	f := fixtures.All[0]
	chat, messages := createFixtureChat(t, parent, f)
	request := &analysisservicepb.GenerateResultsRequest{Parent: chatAnalyses + "-", Resource: chat.GetName()}

	before := countDecisions(t)
	generated, err := analysisServer.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Len(t, generated.GetResults(), 6)
	require.Equal(t, before+2, countDecisions(t))

	// Recording the results left them current.
	again, err := analysisServer.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Equal(t, before+2, countDecisions(t))
	require.Empty(t, cmp.Diff(generated, again, protocmp.Transform(), protocmp.SortRepeated(func(a, b *analysispb.Result) bool { return a.GetName() < b.GetName() })))

	// ListResults reads what the annotations hold.
	listed, err := analysisServer.ListResults(ctx, &analysisservicepb.ListResultsRequest{Parent: chatAnalyses + "-", Filter: fmt.Sprintf("resource = %q", chat.GetName())})
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(generated.GetResults(), listed.GetResults(), protocmp.Transform(), protocmp.SortRepeated(func(a, b *analysispb.Result) bool { return a.GetName() < b.GetName() })))

	// Editing a message makes the transcript analyses stale; untitled stays current.
	last := messages[len(messages)-1]
	last.Blocks = []*aipb.Block{{Content: &aipb.Block_Text{Text: "Wait, it's been 9 days already!"}}}
	_, err = aiServiceClient.UpdateMessage(ctx, &aiservicepb.UpdateMessageRequest{Message: last, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"blocks"}}})
	require.NoError(t, err)
	_, err = analysisServer.GenerateResults(ctx, request)
	require.NoError(t, err)
	require.Equal(t, before+3, countDecisions(t))
}

// An analysis that doesn't apply to the resource is skipped.
func TestAnalysisFilter(t *testing.T) {
	startAnalysis(t)
	f := fixtures.All[1]
	chat, _ := createFixtureChat(t, newUserParent(), f)

	generated, err := analysisServer.GenerateResults(ctx, &analysisservicepb.GenerateResultsRequest{
		Parent: chatAnalyses + "-", Resource: chat.GetName(), Analyses: []string{"polite", "support-tone"},
	})
	require.NoError(t, err)
	require.Len(t, generated.GetResults(), 1)
	require.Equal(t, analysis.ResultName(chatAnalyses+"polite", chat.GetName()), generated.GetResults()[0].GetName())
}

// Recording results emits an update: it is skipped, and other updates leave current results alone.
func TestAnalysisEventsConverge(t *testing.T) {
	startAnalysis(t)
	f := fixtures.All[1]
	chat, _ := createFixtureChat(t, newUserParent(), f)
	publishChatEvent(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED, chat)
	waitHandled(t, chat.GetName(), 1)
	chat = waitForLabels(t, chat.GetName(), len(expectedLabels(f)))
	before := countDecisions(t)

	var recordedPaths []string
	for key := range chat.GetLabels() {
		recordedPaths = append(recordedPaths, "labels."+pbfieldmask.QuoteKey(key), "annotations."+pbfieldmask.QuoteKey(key))
	}
	publishChatEvent(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED, chat, recordedPaths...)
	publishChatEvent(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED, chat, "title")
	waitHandled(t, chat.GetName(), 2)
	require.Equal(t, before, countDecisions(t))

	// A new message makes the transcript analyses stale, and untitled too: its JSON holds the message counts.
	_, err := aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{Parent: chat.GetName(), Message: f.Messages[0]})
	require.NoError(t, err)
	publishChatEvent(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED, chat, "user_message_count")
	waitHandled(t, chat.GetName(), 1)
	require.Equal(t, before+2, countDecisions(t))
}

// Backfill runs on the chats the analysis's label is missing from.
func TestAnalysisBackfill(t *testing.T) {
	startAnalysis(t)
	parent := newUserParent()
	for _, f := range fixtures.All[:3] {
		createFixtureChat(t, parent, f)
	}
	list := func(ctx context.Context, pageToken string) ([]string, string, error) {
		response, err := aiServiceClient.ListChats(ctx, &aiservicepb.ListChatsRequest{
			Parent: parent, Filter: fmt.Sprintf("-labels.%q:*", analysispb.Labels.Analysis.GetKey("polite")), PageSize: 2, PageToken: pageToken,
		})
		var names []string
		for _, chat := range response.GetChats() {
			names = append(names, chat.GetName())
		}
		return names, response.GetNextPageToken(), err
	}

	count, err := analysis.Backfill[*aipb.Chat](ctx, analysisRegistry, list, 2)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	count, err = analysis.Backfill[*aipb.Chat](ctx, analysisRegistry, list, 2)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
