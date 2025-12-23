package pbutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/test/pbutil"
)

func TestValidateMask(t *testing.T) {
	tests := []struct {
		name    string
		paths   string
		wantErr bool
	}{
		{
			name:    "valid single path",
			paths:   "display_name",
			wantErr: false,
		},
		{
			name:    "valid multiple paths",
			paths:   "display_name,shelf_type",
			wantErr: false,
		},
		{
			name:    "valid nested path",
			paths:   "create_time",
			wantErr: false,
		},
		{
			name:    "invalid path",
			paths:   "nonexistent_field",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMask(&pb.Shelf{}, tc.paths)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestApplyMask(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
		CreateTime:  timestamppb.Now(),
	}

	err := ApplyMask(shelf, "display_name")
	require.NoError(t, err)
	require.Equal(t, "Fiction", shelf.DisplayName)
	require.Empty(t, shelf.Name)
	require.Nil(t, shelf.CreateTime)
}

func TestApplyMaskInverse(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
	}

	err := ApplyMaskInverse(shelf, "display_name")
	require.NoError(t, err)
	require.Empty(t, shelf.DisplayName)
	require.Equal(t, "shelves/1", shelf.Name)
}

func TestNestedFieldMask(t *testing.T) {
	mask := MustNewNestedFieldMask(&pb.Shelf{}, "display_name,shelf_type")

	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
	}

	mask.ApplyInverse(shelf)
	require.Empty(t, shelf.DisplayName)
	require.Equal(t, pb.ShelfType_SHELF_TYPE_UNSPECIFIED, shelf.ShelfType)
	require.Equal(t, "shelves/1", shelf.Name)
}

func TestNewNestedFieldMask_Invalid(t *testing.T) {
	_, err := NewNestedFieldMask(&pb.Shelf{}, "invalid_field")
	require.Error(t, err)
}

func TestMustNewNestedFieldMask_Panics(t *testing.T) {
	require.Panics(t, func() {
		MustNewNestedFieldMask(&pb.Shelf{}, "invalid_field")
	})
}

func TestGenerateFieldMaskPaths(t *testing.T) {
	t.Run("default behavior (all possible paths)", func(t *testing.T) {
		shelf := &pb.Shelf{}
		// Without WithOnlySet, it should traverse the entire schema structure
		// regardless of whether fields are set or not.
		paths := GenerateFieldMaskPaths(shelf)
		require.ElementsMatch(t, []string{
			"name",
			"display_name",
			// Timestamp fields are messages, so they recurse to their primitive fields
			"create_time.seconds",
			"create_time.nanos",
			"update_time.seconds",
			"update_time.nanos",
			"shelf_type",
			// Repeated fields are treated as a single path (leaves)
			"books",
			// Nested messages recurse
			"author.name",
			"author.display_name",
			"author.create_time.seconds",
			"author.create_time.nanos",
			"author.update_time.seconds",
			"author.update_time.nanos",
		}, paths)
	})

	t.Run("WithOnlySet populated fields", func(t *testing.T) {
		shelf := &pb.Shelf{
			Name:        "shelves/1",
			DisplayName: "Fiction",
			Author: &pb.Author{
				Name:        "authors/1",
				DisplayName: "John Doe",
			},
		}
		paths := GenerateFieldMaskPaths(shelf, WithOnlySet())
		require.ElementsMatch(t, []string{
			"name",
			"display_name",
			"author.name",
			"author.display_name",
		}, paths)
	})

	t.Run("WithOnlySet explicit empty nested message", func(t *testing.T) {
		// When a nested message is set but empty, it should appear as the parent path
		// because no children are added by the recursion.
		shelf := &pb.Shelf{
			Name:   "shelves/1",
			Author: &pb.Author{},
		}
		paths := GenerateFieldMaskPaths(shelf, WithOnlySet())
		require.ElementsMatch(t, []string{
			"name",
			"author",
		}, paths)
	})

	t.Run("WithOnlySet repeated fields", func(t *testing.T) {
		// Repeated fields present in the message should match the field name.
		shelf := &pb.Shelf{
			Books: []*pb.Book{
				{Title: "Book 1"},
			},
		}
		paths := GenerateFieldMaskPaths(shelf, WithOnlySet())
		require.ElementsMatch(t, []string{
			"books",
		}, paths)
	})
}
