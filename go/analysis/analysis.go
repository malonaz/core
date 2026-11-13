// Package analysis defines analyses: questions or tasks a model answers about resources,
// recorded on each resource as an annotation, and for questions also a label.
package analysis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/uuid"
)

// resultNamespace derives result ids from analysed resource names.
var resultNamespace = uuid.MustParse("6b1f8a51-1c55-4d8e-9a43-2f2c0b7e8d10")

// Output is what the judge must produce.
type Output struct {
	analysis *analysispb.Analysis
}

func Boolean() Output {
	return Output{&analysispb.Analysis{Output: &analysispb.Analysis_Boolean{Boolean: &aipb.BooleanQuestion{}}}}
}

func Choice(optionToDescription map[string]string) Output {
	return Output{&analysispb.Analysis{Output: &analysispb.Analysis_Choice{Choice: &aipb.ChoiceQuestion{Options: optionToDescription}}}}
}

func Score(levels ...string) Output {
	return Output{&analysispb.Analysis{Output: &analysispb.Analysis_Score{Score: &aipb.ScoreQuestion{Levels: levels}}}}
}

func Structured(schema *jsonpb.Schema) Output {
	return Output{&analysispb.Analysis{Output: &analysispb.Analysis_Structured{Structured: schema}}}
}

func Text(maxTokens int32) Output {
	return Output{&analysispb.Analysis{Output: &analysispb.Analysis_Text{Text: &analysispb.TextOutput{MaxTokens: maxTokens}}}}
}

// Analysis is a question or task about resources of type T.
type Analysis[T proto.Message] struct {
	ID           string
	Description  string
	Instructions string
	Output       Output
	// Unset uses the judge's default for the output.
	Model string
	// Whether the analysis runs on a resource. Pure. Nil runs on all.
	Applies func(T) bool
}

// View is what a group of analyses reads. Its analyses share one judge call per model.
type View[T proto.Message, C any] struct {
	// Fetches and renders the input. Deterministic over what it reads: the prompt etag hashes it.
	Input    func(ctx context.Context, clients C, resource T) ([]*aipb.Block, error)
	Analyses []*Analysis[T]
}

// Proto validates the analysis and returns it as a proto.
func (a *Analysis[T]) Proto() (*analysispb.Analysis, error) {
	if a.Output.analysis == nil {
		return nil, fmt.Errorf("missing Output")
	}
	resourceType, err := ResourceType[T]()
	if err != nil {
		return nil, err
	}
	service, kind, _ := strings.Cut(resourceType, "/")
	analysis := proto.Clone(a.Output.analysis).(*analysispb.Analysis)
	analysis.Name = (&analysispb.AnalysisRn{Service: service, ResourceType: kind, Analysis: a.ID}).String()
	analysis.Description = a.Description
	analysis.Instructions = a.Instructions
	analysis.Model = a.Model
	analysis.ResourceType = resourceType
	if isQuestion(analysis) {
		analysis.LabelKey = analysispb.Labels.Analysis.GetKey(a.ID)
	}
	if err := protovalidate.Validate(analysis); err != nil {
		return nil, err
	}
	return analysis, nil
}

// Result reads the analysis's result from a resource.
func (a *Analysis[T]) Result(resource T) (*analysispb.Result, bool, error) {
	annotatable, ok := any(resource).(aip.Annotatable)
	if !ok {
		return nil, false, fmt.Errorf("%T has no annotations", resource)
	}
	return analysispb.Annotations.Analysis(a.ID).Get(annotatable)
}

// ResourceType returns T's resource type, e.g. "ai.malonaz.com/Chat".
func ResourceType[T proto.Message]() (string, error) {
	var zero T
	descriptor := zero.ProtoReflect().Descriptor()
	resource, err := pbutil.GetExtension[*annotationspb.ResourceDescriptor](descriptor.Options(), annotationspb.E_Resource)
	if err != nil {
		return "", fmt.Errorf("%s is not a resource: %w", descriptor.FullName(), err)
	}
	return resource.GetType(), nil
}

// TextBlock returns a text block.
func TextBlock(text string) *aipb.Block {
	return &aipb.Block{Content: &aipb.Block_Text{Text: text}}
}

// ResourceInput is a view input of the resource alone, as ResourceJSON.
func ResourceInput[T proto.Message, C any](_ context.Context, _ C, resource T) ([]*aipb.Block, error) {
	block, err := ResourceJSON(resource)
	if err != nil {
		return nil, err
	}
	return []*aipb.Block{block}, nil
}

// recordedFields change when a result is recorded, so renders leave them out.
var recordedFields = []protoreflect.Name{"labels", "annotations", "etag", "update_time"}

// ResourceJSON renders a resource as stable JSON, without the fields recording a result changes.
func ResourceJSON(resource proto.Message) (*aipb.Block, error) {
	resource = proto.Clone(resource)
	message := resource.ProtoReflect()
	for _, name := range recordedFields {
		if field := message.Descriptor().Fields().ByName(name); field != nil {
			message.Clear(field)
		}
	}
	text, err := StableJSON(resource)
	if err != nil {
		return nil, err
	}
	return TextBlock(text), nil
}

// StableJSON renders protojson, whose whitespace varies by binary, with fixed indentation.
func StableJSON(message proto.Message) (string, error) {
	raw, err := pbutil.JSONMarshal(message)
	if err != nil {
		return "", fmt.Errorf("marshaling json: %w", err)
	}
	var buffer bytes.Buffer
	if err := json.Indent(&buffer, raw, "", "  "); err != nil {
		return "", fmt.Errorf("indenting json: %w", err)
	}
	return buffer.String(), nil
}

func isQuestion(analysis *analysispb.Analysis) bool {
	switch analysis.GetOutput().(type) {
	case *analysispb.Analysis_Boolean, *analysispb.Analysis_Choice, *analysispb.Analysis_Score:
		return true
	}
	return false
}

// question builds the decision question for an analysis.
func question(analysis *analysispb.Analysis) *aipb.Question {
	question := &aipb.Question{Id: idOf(analysis), Instructions: analysis.GetInstructions()}
	switch output := analysis.GetOutput().(type) {
	case *analysispb.Analysis_Boolean:
		question.Kind = &aipb.Question_Boolean{Boolean: output.Boolean}
	case *analysispb.Analysis_Choice:
		question.Kind = &aipb.Question_Choice{Choice: output.Choice}
	case *analysispb.Analysis_Score:
		question.Kind = &aipb.Question_Score{Score: output.Score}
	}
	return question
}

func idOf(analysis *analysispb.Analysis) string {
	return analysis.GetName()[strings.LastIndex(analysis.GetName(), "/")+1:]
}

// PromptEtag hashes the model, the output, the instructions and the input.
func PromptEtag(analysis *analysispb.Analysis, model string, input []*aipb.Block) (string, error) {
	hash := sha256.New()
	hash.Write([]byte(model))
	hash.Write([]byte{0})
	if err := writeMessages(hash, &analysispb.Analysis{Output: analysis.Output, Instructions: analysis.GetInstructions()}); err != nil {
		return "", err
	}
	if err := writeMessages(hash, input...); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)[:16]), nil
}

func writeMessages[M proto.Message](hash interface{ Write([]byte) (int, error) }, messages ...M) error {
	for _, message := range messages {
		wire, err := pbutil.MarshalDeterministic(message)
		if err != nil {
			return fmt.Errorf("marshaling: %w", err)
		}
		fmt.Fprintf(hash, "%d:", len(wire))
		hash.Write(wire)
	}
	return nil
}

// ResultName is the name of an analysis's result on a resource.
func ResultName(analysisName, resource string) string {
	return analysisName + "/results/" + uuid.NewV5(resultNamespace, resource).String()
}

// LabelValue is the label of an answer: the most likely outcome.
func LabelValue(answer *aipb.Answer) (string, bool) {
	switch kind := answer.GetKind().(type) {
	case *aipb.Answer_Boolean:
		return aip.LabelBool(kind.Boolean.GetProbability() >= 0.5), true
	case *aipb.Answer_Choice:
		return kind.Choice.GetOption(), true
	case *aipb.Answer_Score:
		best := 0
		for i, probability := range kind.Score.GetProbabilities() {
			if probability > kind.Score.GetProbabilities()[best] {
				best = i
			}
		}
		return fmt.Sprint(best), true
	}
	return "", false
}
