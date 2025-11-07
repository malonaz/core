package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseIdentifiersFromPattern(t *testing.T) {
	t.Parallel()

	t.Run("topline collection - single identifier", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/{user}")
		require.Len(t, names, 1)
		require.Equal(t, "user", names[0])
	})

	t.Run("nested collection - two identifiers", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/{user}/contacts/{contact}")
		require.Len(t, names, 2)
		require.Equal(t, "user", names[0])
		require.Equal(t, "contact", names[1])
	})

	t.Run("singleton resource - trailing static segment", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/{user}/config")
		require.Len(t, names, 1)
		require.Equal(t, "user", names[0])
	})

	t.Run("no variables in pattern", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("publishers")
		require.Empty(t, names)
	})

	t.Run("complex nested resource", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("projects/{project}/locations/{location}/instances/{instance}")
		require.Len(t, names, 3)
		require.Equal(t, "project", names[0])
		require.Equal(t, "location", names[1])
		require.Equal(t, "instance", names[2])
	})

	t.Run("multiple consecutive identifiers", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("{org}/{team}/{project}")
		require.Len(t, names, 3)
		require.Equal(t, "org", names[0])
		require.Equal(t, "team", names[1])
		require.Equal(t, "project", names[2])
	})

	t.Run("pattern with only static segments", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("api/v1/status")
		require.Empty(t, names)
	})

	t.Run("malformed pattern - unclosed brace", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/{user")
		require.Empty(t, names)
	})

	t.Run("malformed pattern - unmatched closing brace", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/user}")
		require.Empty(t, names)
	})

	t.Run("empty pattern", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("")
		require.Empty(t, names)
	})

	t.Run("child of nested singleton resource", func(t *testing.T) {
		t.Parallel()
		names := parseIdentifiersFromPattern("users/{user}/config/files/{file}")
		require.Len(t, names, 2)
		require.Equal(t, "user", names[0])
		require.Equal(t, "file", names[1])
	})
}
