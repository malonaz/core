package schema

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
)

// Descendant is a persisted resource beneath another, addressed through that
// ancestor's identifiers: every descendant pattern extends the ancestor's, so
// the ancestor's identifier columns are a prefix of the descendant's.
type Descendant struct {
	Resource  *resource.ParsedResource
	Pattern   *resource.ParsedPattern
	Message   *protogen.Message
	ModelOpts *modelpb.ModelOpts
	// Gating is true for collection resources reached from the ancestor
	// through singletons only: their live rows block the ancestor's deletion
	// unless it is forced (AIP-135). Singletons share their parent's
	// lifecycle and never gate, nor do resources beneath a gating one.
	Gating bool
	// SoftDelete is true when a cascade from the ancestor tombstones the
	// descendant instead of removing its rows: the ancestor and every
	// resource on the path down must be soft-deletable, or the rows would
	// outlive a hard-deleted parent.
	SoftDelete bool
	// HasDeleteTime is true when the descendant is itself soft-deletable,
	// in which case only its live rows count as present.
	HasDeleteTime bool
}

// Table resolves the descendant's table.
func (d Descendant) Table() Table {
	return TableOf(d.Resource, d.ModelOpts)
}

// Descendants returns every persisted resource beneath a resource, deepest
// first so that hard deletes respect foreign keys. Silent resources — those
// without a message or model options — have no rows of their own but are
// traversed, since their descendants still carry the ancestor's identifiers.
func Descendants(parentMessage *protogen.Message, parent *resource.ParsedResource) ([]Descendant, error) {
	var descendants []Descendant
	for _, parentPattern := range parent.Patterns {
		if err := collectDescendants(parentPattern, hasDeleteTime(parentMessage), true, &descendants); err != nil {
			return nil, err
		}
	}
	return descendants, nil
}

// AnyGating reports whether live descendants can block a deletion.
func AnyGating(descendants []Descendant) bool {
	for _, descendant := range descendants {
		if descendant.Gating {
			return true
		}
	}
	return false
}

func collectDescendants(parentPattern *resource.ParsedPattern, parentSoft, gating bool, out *[]Descendant) error {
	for _, child := range parentPattern.Resource.Children {
		for _, pattern := range child.Patterns {
			if pattern.Parent == nil || pattern.Parent.Value != parentPattern.Value {
				continue
			}
			descendant, err := resolveDescendant(child, pattern, parentSoft, gating)
			if err != nil {
				return err
			}
			childSoft, childGating := parentSoft, gating
			if descendant != nil {
				childSoft = descendant.SoftDelete
				childGating = pattern.Singleton && gating
			}
			if err := collectDescendants(pattern, childSoft, childGating, out); err != nil {
				return err
			}
			if descendant != nil {
				*out = append(*out, *descendant)
			}
		}
	}
	return nil
}

// resolveDescendant returns nil for silent resources.
func resolveDescendant(child *resource.ParsedResource, pattern *resource.ParsedPattern, parentSoft, gating bool) (*Descendant, error) {
	message, err := resource.GetMessageByResourceType(child.Desc.Type)
	if err != nil {
		return nil, nil
	}
	modelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting model_opts for descendant %s: %w", child.Desc.Type, err)
	}
	return &Descendant{
		Resource:      child,
		Pattern:       pattern,
		Message:       message,
		ModelOpts:     modelOpts,
		Gating:        gating && !pattern.Singleton,
		SoftDelete:    parentSoft && hasDeleteTime(message),
		HasDeleteTime: hasDeleteTime(message),
	}, nil
}

func hasDeleteTime(message *protogen.Message) bool {
	return message.Desc.Fields().ByName("delete_time") != nil
}
