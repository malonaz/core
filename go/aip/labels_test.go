package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestSetLabel(t *testing.T) {
	t.Run("OnNilLabels", func(t *testing.T) {
		author := &librarypb.Author{}
		SetLabel(author, "env", "production")
		require.Equal(t, "production", author.GetLabels()["env"])
	})

	t.Run("OnExistingLabels", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"existing": "value"}}
		SetLabel(author, "env", "staging")
		require.Equal(t, "staging", author.GetLabels()["env"])
		require.Equal(t, "value", author.GetLabels()["existing"])
	})

	t.Run("OverwritesExistingKey", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"env": "old"}}
		SetLabel(author, "env", "new")
		require.Equal(t, "new", author.GetLabels()["env"])
	})
}

func TestGetLabel(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"tier": "premium"}}
		value, ok := GetLabel(author, "tier")
		require.True(t, ok)
		require.Equal(t, "premium", value)
	})

	t.Run("NotExists", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"tier": "premium"}}
		value, ok := GetLabel(author, "missing")
		require.False(t, ok)
		require.Empty(t, value)
	})

	t.Run("NilLabels", func(t *testing.T) {
		author := &librarypb.Author{}
		value, ok := GetLabel(author, "any")
		require.False(t, ok)
		require.Empty(t, value)
	})
}

func TestHasLabel(t *testing.T) {
	t.Run("True", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"env": "prod"}}
		require.True(t, HasLabel(author, "env"))
	})

	t.Run("False", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"env": "prod"}}
		require.False(t, HasLabel(author, "missing"))
	})
}

func TestDeleteLabel(t *testing.T) {
	t.Run("ExistingKey", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"env": "prod", "tier": "gold"}}
		DeleteLabel(author, "env")
		require.False(t, HasLabel(author, "env"))
		require.True(t, HasLabel(author, "tier"))
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		author := &librarypb.Author{Labels: map[string]string{"env": "prod"}}
		DeleteLabel(author, "missing")
		require.True(t, HasLabel(author, "env"))
	})

	t.Run("NilLabels", func(t *testing.T) {
		author := &librarypb.Author{}
		DeleteLabel(author, "any")
	})
}

func TestLabelBool(t *testing.T) {
	require.Equal(t, LabelValueTrue, LabelBool(true))
	require.Equal(t, LabelValueFalse, LabelBool(false))
}
