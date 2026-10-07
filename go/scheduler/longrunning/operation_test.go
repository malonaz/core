package longrunning

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobParentOf(t *testing.T) {
	for resource, expected := range map[string]string{
		"organizations/o/users/u/things/t": "organizations/o/users/u",
		"organizations/o/contacts/c":       "organizations/o",
		"organizations/o/users/-/things/t": "organizations/o",
		"organizations/-/contacts/-":       "",
		"organizations/-/users/u/things/t": "",
		"lenders/-":                        "",
	} {
		require.Equal(t, expected, jobParentOf(resource), resource)
	}
}
