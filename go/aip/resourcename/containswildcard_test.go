package resourcename

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsWildcard(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "empty",
			input:    "",
			expected: false,
		},

		{
			name:     "singleton",
			input:    "foo",
			expected: false,
		},

		{
			name:     "singleton wildcard",
			input:    "-",
			expected: true,
		},

		{
			name:     "multi",
			input:    "foo/bar",
			expected: false,
		},

		{
			name:     "multi wildcard at start",
			input:    "-/bar",
			expected: true,
		},

		{
			name:     "multi wildcard at end",
			input:    "foo/-",
			expected: true,
		},

		{
			name:     "multi wildcard at middle",
			input:    "foo/-/bar",
			expected: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, ContainsWildcard(tt.input))
		})
	}
}
