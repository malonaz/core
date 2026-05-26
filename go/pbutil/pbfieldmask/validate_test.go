package pbfieldmask

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		paths   []string
		wantErr bool
	}{
		{"valid single", []string{"name"}, false},
		{"valid enum", []string{"genre"}, false},
		{"valid nested", []string{"metadata.capacity"}, false},
		{"valid multiple", []string{"name", "display_name", "genre"}, false},
		{"valid map", []string{"labels"}, false},
		{"valid map key", []string{"labels.my_key"}, false},
		{"valid map backtick key", []string{"labels.`my key`"}, false},
		{"valid map backtick key with dots", []string{"labels.`my.dotted.key`"}, false},
		{"valid wildcard", []string{"*"}, false},
		{"invalid field", []string{"nonexistent"}, true},
		{"invalid nested", []string{"metadata.nonexistent"}, true},
		{"invalid deep", []string{"metadata.capacity.foo"}, true},
		{"wildcard with others", []string{"*", "name"}, true},
		{"invalid map value subfield on string map", []string{"labels.my_key.foo"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := FromPaths(tc.paths...).Validate(&pb.Shelf{})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
