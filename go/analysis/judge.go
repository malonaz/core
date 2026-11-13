package analysis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
)

var log = slog.Default().With("package", "analysis")

// Config configures the judge.
type Config struct {
	// Judge calls are created under this user unless the resource names one.
	// Format: organizations/{organization}/users/{user}
	Parent string
	// The default model of questions.
	DecisionModel string
	// The default model of text.
	ChatModel string
}

// Judge asks the AI service.
type Judge struct {
	client       aiservicepb.AiServiceClient
	config       *Config
	decisionHook func(*aipb.Decision) error
	messageHook  func(*aipb.Message) error
}

type JudgeOpt func(*Judge)

// WithDecisionHook edits every decision before it is created, e.g. to script the mock model.
func WithDecisionHook(hook func(*aipb.Decision) error) JudgeOpt {
	return func(j *Judge) { j.decisionHook = hook }
}

// WithMessageHook edits the input message of every text generation, e.g. to script the mock model.
func WithMessageHook(hook func(*aipb.Message) error) JudgeOpt {
	return func(j *Judge) { j.messageHook = hook }
}

// NewJudge returns a judge calling the AI service.
func NewJudge(client aiservicepb.AiServiceClient, config *Config, opts ...JudgeOpt) *Judge {
	judge := &Judge{client: client, config: config}
	for _, opt := range opts {
		opt(judge)
	}
	return judge
}

// model is the model an analysis is judged with.
func (j *Judge) model(analysis *analysispb.Analysis) string {
	if analysis.GetModel() != "" {
		return analysis.GetModel()
	}
	if isQuestion(analysis) {
		return j.config.DecisionModel
	}
	return j.config.ChatModel
}

// checkModel fails when an analysis's model cannot produce its output.
func (j *Judge) checkModel(ctx context.Context, analysis *analysispb.Analysis) error {
	if analysis.GetStructured() != nil {
		return fmt.Errorf("%s: structured output is not implemented", analysis.GetName())
	}
	model, err := j.client.GetModel(ctx, &aiservicepb.GetModelRequest{Name: j.model(analysis)})
	if err != nil {
		return fmt.Errorf("%s: getting model: %w", analysis.GetName(), err)
	}
	if isQuestion(analysis) && model.GetDecision() == nil {
		return fmt.Errorf("%s: %s is not a decision model", analysis.GetName(), model.GetName())
	}
	if !isQuestion(analysis) && model.GetTtt() == nil {
		return fmt.Errorf("%s: %s is not a text model", analysis.GetName(), model.GetName())
	}
	return nil
}

// item is one analysis due in a judge call.
type item struct {
	analysis *analysispb.Analysis
	etag     string
}

// decide asks one decision about items sharing a model and an input. A refused question has no result.
func (j *Judge) decide(ctx context.Context, parent, model string, input []*aipb.Block, items []item) (map[string]*analysispb.Result, error) {
	decision := &aipb.Decision{Model: model, Input: input}
	for _, item := range items {
		decision.Questions = append(decision.Questions, question(item.analysis))
	}
	if j.decisionHook != nil {
		if err := j.decisionHook(decision); err != nil {
			return nil, fmt.Errorf("decision hook: %w", err)
		}
	}
	decision, err := j.client.CreateDecision(ctx, &aiservicepb.CreateDecisionRequest{Parent: parent, Decision: decision})
	if err != nil {
		return nil, err
	}
	idToAnswer := make(map[string]*aipb.Answer, len(decision.GetAnswers()))
	for _, answer := range decision.GetAnswers() {
		idToAnswer[answer.GetQuestionId()] = answer
	}
	idToResult := make(map[string]*analysispb.Result, len(items))
	for _, item := range items {
		id := idOf(item.analysis)
		answer := idToAnswer[id]
		if answer == nil || answer.GetRefusal() != nil {
			log.WarnContext(ctx, "question refused", "analysis", item.analysis.GetName(), "decision", decision.GetName())
			continue
		}
		idToResult[id] = &analysispb.Result{
			PromptEtag:  item.etag,
			Value:       &analysispb.Result_Answer{Answer: proto.Clone(answer).(*aipb.Answer)},
			Model:       model,
			Source:      decision.GetName(),
			AnalyzeTime: decision.GetCreateTime(),
		}
	}
	return idToResult, nil
}

// generate writes one text, in a chat of its own.
func (j *Judge) generate(ctx context.Context, parent, model string, input []*aipb.Block, item item) (*analysispb.Result, error) {
	chat, err := j.client.CreateChat(ctx, &aiservicepb.CreateChatRequest{Parent: parent, Chat: &aipb.Chat{Title: item.analysis.GetName()}})
	if err != nil {
		return nil, fmt.Errorf("creating chat: %w", err)
	}
	user := &aipb.Message{Role: aipb.Role_ROLE_USER, Blocks: input}
	if j.messageHook != nil {
		if err := j.messageHook(user); err != nil {
			return nil, fmt.Errorf("message hook: %w", err)
		}
	}
	response, err := j.client.GenerateMessage(ctx, &aiservicepb.GenerateMessageRequest{
		Parent: chat.GetName(),
		Model:  model,
		Messages: []*aipb.Message{
			{Role: aipb.Role_ROLE_SYSTEM, Blocks: []*aipb.Block{TextBlock(item.analysis.GetInstructions())}},
			user,
		},
		Configuration: &aiservicepb.MessageGenerationConfiguration{MaxTokens: item.analysis.GetText().GetMaxTokens()},
	})
	if err != nil {
		return nil, err
	}
	var texts []string
	for _, block := range response.GetGeneratedMessage().GetBlocks() {
		if text := block.GetText(); text != "" {
			texts = append(texts, text)
		}
	}
	if len(texts) == 0 {
		return nil, fmt.Errorf("%s: no text generated in %s", item.analysis.GetName(), chat.GetName())
	}
	return &analysispb.Result{
		PromptEtag:  item.etag,
		Value:       &analysispb.Result_Text{Text: strings.Join(texts, "\n")},
		Model:       model,
		Source:      chat.GetName(),
		AnalyzeTime: response.GetGeneratedMessage().GetCreateTime(),
	}, nil
}

// judge asks one analysis.
func (j *Judge) judge(ctx context.Context, parent, model string, input []*aipb.Block, it item) (*analysispb.Result, error) {
	if !isQuestion(it.analysis) {
		return j.generate(ctx, parent, model, input, it)
	}
	idToResult, err := j.decide(ctx, parent, model, input, []item{it})
	if err != nil {
		return nil, err
	}
	result, ok := idToResult[idOf(it.analysis)]
	if !ok {
		return nil, fmt.Errorf("%s: refused", it.analysis.GetName())
	}
	return result, nil
}

// Evaluate asks the judge an analysis over an input, without recording anything.
func Evaluate[T proto.Message](ctx context.Context, judge *Judge, analysis *Analysis[T], input []*aipb.Block) (*analysispb.Result, error) {
	analysisProto, err := analysis.Proto()
	if err != nil {
		return nil, err
	}
	model := judge.model(analysisProto)
	etag, err := PromptEtag(analysisProto, model, input)
	if err != nil {
		return nil, err
	}
	result, err := judge.judge(ctx, judge.config.Parent, model, input, item{analysis: analysisProto, etag: etag})
	if err != nil {
		return nil, err
	}
	if result.AnalyzeTime == nil {
		result.AnalyzeTime = timestamppb.New(time.Now())
	}
	return result, nil
}
