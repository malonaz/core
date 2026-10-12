package eval

import (
	"fmt"

	evalpb "github.com/malonaz/core/genproto/ai/eval/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// Recordable is a resource evaluations are recorded on.
type Recordable interface {
	aip.Labellable
	aip.Annotatable
}

// Record records evaluations on a resource, each under its eval's key: the verdict as a
// label, to filter on, and the evaluation as an annotation. Persist them with an update
// masked by UpdateMask, which touches no other key.
func Record(resource Recordable, evaluations ...*evalpb.Evaluation) error {
	for _, evaluation := range evaluations {
		verdict, err := verdictMapping.ToExternal(evaluation.GetVerdict())
		if err != nil {
			return fmt.Errorf("recording eval %q: %w", evaluation.GetEval(), err)
		}
		aip.SetLabel(resource, evalpb.Labels.Eval.GetKey(evaluation.GetEval()), verdict)
		if err := evalpb.Annotations.Eval(evaluation.GetEval()).Set(resource, evaluation); err != nil {
			return fmt.Errorf("recording eval %q: %w", evaluation.GetEval(), err)
		}
	}
	return nil
}

// UpdateMask returns the paths Record writes for evaluations.
func UpdateMask(evaluations ...*evalpb.Evaluation) *pbfieldmask.FieldMask {
	paths := make([]string, 0, 2*len(evaluations))
	for _, evaluation := range evaluations {
		key := pbfieldmask.QuoteKey(evalpb.Labels.Eval.GetKey(evaluation.GetEval()))
		paths = append(paths, "labels."+key, "annotations."+key)
	}
	return pbfieldmask.FromPaths(paths...)
}
