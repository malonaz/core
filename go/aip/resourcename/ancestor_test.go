package resourcename

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAncestor(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		input      string
		pattern    string
		expected   string
		expectedOK bool
	}{
		{
			name:     "empty all",
			input:    "",
			pattern:  "",
			expected: "",
		},
		{
			name:     "empty pattern",
			input:    "foo/1/bar/2",
			pattern:  "",
			expected: "",
		},

		{
			name:     "empty name",
			input:    "",
			pattern:  "foo/{foo}",
			expected: "",
		},

		{
			name:     "non-matching pattern",
			input:    "foo/1/bar/2",
			pattern:  "baz/{baz}",
			expected: "",
		},

		{
			name:       "ok",
			input:      "foo/1/bar/2",
			pattern:    "foo/{foo}",
			expected:   "foo/1",
			expectedOK: true,
		},

		{
			name:       "ok full",
			input:      "//foo.example.com/foo/1/bar/2",
			pattern:    "foo/{foo}",
			expected:   "//foo.example.com/foo/1",
			expectedOK: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual, ok := Ancestor(tt.input, tt.pattern)
			require.Equal(t, tt.expectedOK, ok)
			require.Equal(t, tt.expected, actual)
		})
	}
}
