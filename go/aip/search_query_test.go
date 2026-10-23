package aip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPrefixTSQuery(t *testing.T) {
	t.Parallel()
	require.Equal(t, "", BuildPrefixTSQuery(""))
	require.Equal(t, "", BuildPrefixTSQuery("  \t &|!() "))
	require.Equal(t, "john:*", BuildPrefixTSQuery("John"))
	require.Equal(t, "john:* & smi:*", BuildPrefixTSQuery("john smi"))
	require.Equal(t, "john:* & gmail:* & com:*", BuildPrefixTSQuery("john@gmail.com"))
	require.Equal(t, "1:* & 415:* & 555:*", BuildPrefixTSQuery("+1 (415) 555"))
	// Sequential ids: "#" is a separator.
	require.Equal(t, "1042:*", BuildPrefixTSQuery("#1042"))
	require.Equal(t, "invoice:* & 1042:*", BuildPrefixTSQuery("Invoice #1042"))
	// Token cap.
	require.Equal(t,
		"a:* & b:* & c:* & d:* & e:* & f:* & g:* & h:*",
		BuildPrefixTSQuery("a b c d e f g h i j"))
}
