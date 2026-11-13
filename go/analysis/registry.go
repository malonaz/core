package analysis

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// Resource adapts a resource type for analysis. C is the domain's clients struct.
type Resource[T proto.Message, C any] struct {
	Get    func(ctx context.Context, clients C, name string) (T, error)
	Update func(ctx context.Context, clients C, resource T, updateMask *fieldmaskpb.FieldMask) (T, error)
	// The user judge calls on a resource are created under. Nil uses Config.Parent.
	Parent func(resource T) string
}

// view is a View with its types erased.
type view struct {
	input func(ctx context.Context, resource proto.Message) ([]*aipb.Block, error)
}

// registered is an analysis with its types erased.
type registered struct {
	proto   *analysispb.Analysis
	view    *view
	applies func(resource proto.Message) bool
}

// resourceType is a registered resource type with its types erased.
type resourceType struct {
	name         string
	get          func(ctx context.Context, name string) (proto.Message, error)
	update       func(ctx context.Context, resource proto.Message, updateMask *fieldmaskpb.FieldMask) error
	parent       func(resource proto.Message) string
	idToAnalysis map[string]*registered
	ids          []string
}

// Registry holds the analyses one binary hosts.
type Registry struct {
	judge          *Judge
	typeToResource map[string]*resourceType
}

func NewRegistry(judge *Judge) *Registry {
	return &Registry{judge: judge, typeToResource: map[string]*resourceType{}}
}

// Register adds views of type T with the clients their adapter and inputs use.
func Register[T proto.Message, C any](ctx context.Context, registry *Registry, resource Resource[T, C], clients C, views ...View[T, C]) error {
	typeName, err := ResourceType[T]()
	if err != nil {
		return err
	}
	if _, ok := registry.typeToResource[typeName]; ok {
		return fmt.Errorf("%s: already registered", typeName)
	}
	rt := &resourceType{
		name: typeName,
		get: func(ctx context.Context, name string) (proto.Message, error) {
			return resource.Get(ctx, clients, name)
		},
		update: func(ctx context.Context, message proto.Message, updateMask *fieldmaskpb.FieldMask) error {
			_, err := resource.Update(ctx, clients, message.(T), updateMask)
			return err
		},
		parent:       func(proto.Message) string { return registry.judge.config.Parent },
		idToAnalysis: map[string]*registered{},
	}
	if resource.Parent != nil {
		rt.parent = func(message proto.Message) string { return resource.Parent(message.(T)) }
	}
	for _, v := range views {
		if v.Input == nil {
			return fmt.Errorf("%s: view has no Input", typeName)
		}
		erased := &view{input: func(ctx context.Context, message proto.Message) ([]*aipb.Block, error) {
			return v.Input(ctx, clients, message.(T))
		}}
		for _, analysis := range v.Analyses {
			analysisProto, err := analysis.Proto()
			if err != nil {
				return fmt.Errorf("analysis %q: %w", analysis.ID, err)
			}
			if _, ok := rt.idToAnalysis[analysis.ID]; ok {
				return fmt.Errorf("%s: id already registered", analysisProto.GetName())
			}
			if err := registry.judge.checkModel(ctx, analysisProto); err != nil {
				return err
			}
			applies := func(proto.Message) bool { return true }
			if analysis.Applies != nil {
				applies = func(message proto.Message) bool { return analysis.Applies(message.(T)) }
			}
			rt.idToAnalysis[analysis.ID] = &registered{proto: analysisProto, view: erased, applies: applies}
			rt.ids = append(rt.ids, analysis.ID)
		}
	}
	slices.Sort(rt.ids)
	registry.typeToResource[typeName] = rt
	return nil
}

// runStats counts what a run did.
type runStats struct {
	skipped int
	current int
	calls   int
}

// run gets, renders, judges and records analyses on one resource. Current results are reused.
func (r *Registry) run(ctx context.Context, rt *resourceType, resourceName string, analyses []*registered, applyFilter bool) ([]*analysispb.Result, runStats, error) {
	var stats runStats
	resource, err := rt.get(ctx, resourceName)
	if err != nil {
		return nil, stats, fmt.Errorf("getting %s: %w", resourceName, err)
	}
	if applyFilter {
		analyses = slices.DeleteFunc(slices.Clone(analyses), func(analysis *registered) bool { return !analysis.applies(resource) })
	}
	stats.skipped = len(rt.ids) - len(analyses)

	// One decision per view and model; one generation per text analysis.
	type call struct {
		model string
		input []*aipb.Block
		items []item
	}
	var calls []*call
	keyToCall := map[string]*call{}
	viewToInput := map[*view][]*aipb.Block{}
	var results []*analysispb.Result
	for _, analysis := range analyses {
		input, ok := viewToInput[analysis.view]
		if !ok {
			if input, err = analysis.view.input(ctx, resource); err != nil {
				return nil, stats, fmt.Errorf("%s: input: %w", analysis.proto.GetName(), err)
			}
			viewToInput[analysis.view] = input
		}
		model := r.judge.model(analysis.proto)
		etag, err := PromptEtag(analysis.proto, model, input)
		if err != nil {
			return nil, stats, err
		}
		current, ok, err := analysispb.Annotations.Analysis(idOf(analysis.proto)).Get(resource.(aip.Annotatable))
		if err != nil {
			return nil, stats, err
		}
		if ok && current.GetPromptEtag() == etag {
			stats.current++
			results = append(results, current)
			continue
		}
		key := fmt.Sprintf("%p\x00%s", analysis.view, model)
		if !isQuestion(analysis.proto) {
			key += "\x00" + analysis.proto.GetName()
		}
		c, ok := keyToCall[key]
		if !ok {
			c = &call{model: model, input: input}
			keyToCall[key] = c
			calls = append(calls, c)
		}
		c.items = append(c.items, item{analysis: analysis.proto, etag: etag})
	}

	parent := rt.parent(resource)
	var judged []*analysispb.Result
	var errs []error
	for _, c := range calls {
		stats.calls++
		var idToResult map[string]*analysispb.Result
		if isQuestion(c.items[0].analysis) {
			idToResult, err = r.judge.decide(ctx, parent, c.model, c.input, c.items)
		} else {
			var result *analysispb.Result
			if result, err = r.judge.generate(ctx, parent, c.model, c.input, c.items[0]); err == nil {
				idToResult = map[string]*analysispb.Result{idOf(c.items[0].analysis): result}
			}
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, item := range c.items {
			result, ok := idToResult[idOf(item.analysis)]
			if !ok {
				continue
			}
			result.Name = ResultName(item.analysis.GetName(), resourceName)
			result.Resource = resourceName
			judged = append(judged, result)
		}
	}
	if len(judged) > 0 {
		if err := r.record(ctx, rt, resource, judged); err != nil {
			return nil, stats, err
		}
	}
	return append(results, judged...), stats, errors.Join(errs...)
}

// record sets each result's annotation, and label for questions, updating only those keys.
func (r *Registry) record(ctx context.Context, rt *resourceType, resource proto.Message, results []*analysispb.Result) error {
	resource = proto.Clone(resource)
	var paths []string
	for _, result := range results {
		rn, err := analysispb.ParseResultRn(result.GetName())
		if err != nil {
			return err
		}
		if err := analysispb.Annotations.Analysis(rn.Analysis).Set(resource.(aip.Annotatable), result); err != nil {
			return err
		}
		key := analysispb.Labels.Analysis.GetKey(rn.Analysis)
		paths = append(paths, "annotations."+pbfieldmask.QuoteKey(key))
		if value, ok := LabelValue(result.GetAnswer()); ok {
			aip.SetLabel(resource.(aip.Labellable), key, value)
			paths = append(paths, "labels."+pbfieldmask.QuoteKey(key))
		}
	}
	if err := rt.update(ctx, resource, &fieldmaskpb.FieldMask{Paths: paths}); err != nil {
		return fmt.Errorf("recording results: %w", err)
	}
	return nil
}

// lookup returns the registered analyses named, all of one type.
func (r *Registry) lookup(names []string) (*resourceType, []*registered, error) {
	var rt *resourceType
	var analyses []*registered
	for _, name := range names {
		rn, err := analysispb.ParseAnalysisRn(name)
		if err != nil {
			return nil, nil, err
		}
		candidate, ok := r.typeToResource[rn.Service+"/"+rn.ResourceType]
		if !ok {
			return nil, nil, fmt.Errorf("%s: resource type not registered", name)
		}
		if rt != nil && rt != candidate {
			return nil, nil, fmt.Errorf("%s: analyses of several resource types", name)
		}
		rt = candidate
		analysis, ok := rt.idToAnalysis[rn.Analysis]
		if !ok {
			return nil, nil, fmt.Errorf("%s: not registered", name)
		}
		analyses = append(analyses, analysis)
	}
	return rt, analyses, nil
}
