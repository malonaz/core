package tool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

func TestPruneArguments(t *testing.T) {
	t.Run("drops hallucinated sibling outside the mask", func(t *testing.T) {
		// Regression: model emitted update_mask as an object on an
		// UpdateContactRequest whose schema only exposed contact.metadata.insights,
		// crashing BuildMessage before mask application.
		fieldMask := pbfieldmask.FromPaths("contact.metadata.insights")
		arguments := map[string]any{
			"update_mask": map[string]any{"paths": []any{"contact"}},
			"contact": map[string]any{
				"metadata": map[string]any{
					"insights": []any{"prefers morning appointments"},
				},
			},
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, map[string]any{
			"contact": map[string]any{
				"metadata": map[string]any{
					"insights": []any{"prefers morning appointments"},
				},
			},
		}, prunedArguments)
	})

	t.Run("strips disallowed siblings at every ancestor level", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("contact.metadata.insights")
		arguments := map[string]any{
			"contact": map[string]any{
				"name": "contacts/123",
				"metadata": map[string]any{
					"insights":       []any{"note"},
					"phone_number":   "+15551234567",
					"classification": "CONTACT_CLASSIFICATION_LEGITIMATE",
				},
			},
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, map[string]any{
			"contact": map[string]any{
				"metadata": map[string]any{
					"insights": []any{"note"},
				},
			},
		}, prunedArguments)
	})

	t.Run("keeps entire subtree under a mask path", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("contact_address.metadata.postal_address")
		arguments := map[string]any{
			"contact_address": map[string]any{
				"metadata": map[string]any{
					"postal_address": map[string]any{
						"region_code":   "US",
						"address_lines": []any{"1 Main St"},
					},
				},
			},
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, arguments, prunedArguments)
	})

	t.Run("keeps multiple leaf mask paths and drops the rest", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths(
			"contact_point.name",
			"contact_point.given_name",
		)
		arguments := map[string]any{
			"contact_point": map[string]any{
				"name":         "contacts/123/points/456",
				"given_name":   "Jane",
				"family_name":  "Doe",
				"phone_number": "+15551234567",
			},
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, map[string]any{
			"contact_point": map[string]any{
				"name":       "contacts/123/points/456",
				"given_name": "Jane",
			},
		}, prunedArguments)
	})

	t.Run("prunes each element of a list at an ancestor position", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("items.title")
		arguments := map[string]any{
			"items": []any{
				map[string]any{"title": "a", "quantity": float64(1)},
				map[string]any{"title": "b"},
				"not-an-object", // Non-object elements are passed through untouched.
			},
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, map[string]any{
			"items": []any{
				map[string]any{"title": "a"},
				map[string]any{"title": "b"},
				"not-an-object",
			},
		}, prunedArguments)
	})

	t.Run("drops scalar where the mask expects a subtree", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("contact.metadata.insights")
		arguments := map[string]any{
			"contact": "contacts/123", // Cannot contain metadata.insights.
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Empty(t, prunedArguments)
	})

	t.Run("wildcard mask keeps everything", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths(pbfieldmask.WildcardPath)
		arguments := map[string]any{
			"anything": map[string]any{"goes": true},
			"here":     float64(1),
		}
		prunedArguments := pruneArguments(arguments, "", fieldMask)
		require.Equal(t, arguments, prunedArguments)
	})

	t.Run("empty arguments", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("contact.metadata.insights")
		require.Empty(t, pruneArguments(map[string]any{}, "", fieldMask))
		require.Empty(t, pruneArguments(nil, "", fieldMask))
	})
}

func TestMaskAllowsSubtree(t *testing.T) {
	fieldMask := pbfieldmask.FromPaths("contact.metadata.insights")

	t.Run("exact path", func(t *testing.T) {
		require.True(t, maskAllowsSubtree("contact.metadata.insights", fieldMask))
	})

	t.Run("descendant of mask path", func(t *testing.T) {
		require.True(t, maskAllowsSubtree("contact.metadata.insights.anything", fieldMask))
	})

	t.Run("ancestor of mask path", func(t *testing.T) {
		require.False(t, maskAllowsSubtree("contact", fieldMask))
		require.False(t, maskAllowsSubtree("contact.metadata", fieldMask))
	})

	t.Run("unrelated path", func(t *testing.T) {
		require.False(t, maskAllowsSubtree("update_mask", fieldMask))
	})

	t.Run("prefix that is not a path segment boundary", func(t *testing.T) {
		// "contact.metadata.insights_extra" must not match "contact.metadata.insights".
		require.False(t, maskAllowsSubtree("contact.metadata.insights_extra", fieldMask))
	})

	t.Run("wildcard", func(t *testing.T) {
		wildcardMask := pbfieldmask.FromPaths(pbfieldmask.WildcardPath)
		require.True(t, maskAllowsSubtree("anything.at.all", wildcardMask))
	})
}
