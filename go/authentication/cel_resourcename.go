package authentication

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"go.einride.tech/aip/resourcename"
)

// resourceNameCelOptions exposes wildcard-aware AIP-122 resource name helpers
// to authorization CEL expressions, so configs never do string surgery on names.
var resourceNameCelOptions = []cel.EnvOption{
	// rn.equals(a, b): segment-wise equality, tolerant of "-" wildcards and @revisions.
	cel.Function("rn.equals",
		cel.Overload("rn_equals_string_string",
			[]*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(func(pattern, name ref.Val) ref.Val {
				return types.Bool(resourcename.Match(pattern.Value().(string), name.Value().(string)))
			}),
		),
	),
	// rn.hasParent(name, parent): true if parent is an ancestor of name, at any depth.
	cel.Function("rn.hasParent",
		cel.Overload("rn_hasParent_string_string",
			[]*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(func(name, parent ref.Val) ref.Val {
				return types.Bool(resourcename.HasParent(name.Value().(string), parent.Value().(string)))
			}),
		),
	),
	// rn.ancestor(name, pattern): extracts an ancestor by pattern,
	// e.g. rn.ancestor(subject, "organizations/{organization}"). Empty string if absent.
	cel.Function("rn.ancestor",
		cel.Overload("rn_ancestor_string_string",
			[]*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
			cel.BinaryBinding(func(name, pattern ref.Val) ref.Val {
				ancestor, ok := resourcename.Ancestor(name.Value().(string), pattern.Value().(string))
				if !ok {
					return types.String("")
				}
				return types.String(ancestor)
			}),
		),
	),
	// rn.parent(name, collectionID): prefix of name before the given collection segment,
	// without needing to know the full pattern. Empty string if absent.
	cel.Function("rn.parent",
		cel.Overload("rn_parent_string_string",
			[]*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
			cel.BinaryBinding(func(name, collectionID ref.Val) ref.Val {
				return types.String(resourceNameParent(name.Value().(string), collectionID.Value().(string)))
			}),
		),
	),
}

func resourceNameParent(name, collectionID string) string {
	scanner := &resourcename.Scanner{}
	scanner.Init(name)
	for scanner.Scan() {
		// Start() > 0 guards against the collection being the root segment (no parent).
		if string(scanner.Segment()) == collectionID && scanner.Start() > 0 {
			return name[:scanner.Start()-1]
		}
	}
	return ""
}
