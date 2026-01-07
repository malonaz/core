package pbfieldmask

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/test/pbutil"
)

func TestFromFieldMask(t *testing.T) {
	fm := &fieldmaskpb.FieldMask{Paths: []string{"display_name", "name"}}
	m := FromFieldMask(fm)
	require.ElementsMatch(t, []string{"display_name", "name"}, m.GetPaths())
}

func TestFromPaths(t *testing.T) {
	m := FromPaths("name", "display_name")
	require.ElementsMatch(t, []string{"name", "display_name"}, m.GetPaths())
}

func TestFromString(t *testing.T) {
	m := FromString("name,display_name,author.name")
	require.ElementsMatch(t, []string{"name", "display_name", "author.name"}, m.GetPaths())
}

func TestFromString_Empty(t *testing.T) {
	m := FromString("")
	require.Equal(t, []string{""}, m.GetPaths())
}

func TestWithParent(t *testing.T) {
	m := FromPaths("name", "display_name").WithParent("shelf")
	require.ElementsMatch(t, []string{"shelf.name", "shelf.display_name"}, m.GetPaths())
}

func TestProto(t *testing.T) {
	m := FromPaths("name")
	require.NotNil(t, m.Proto())
	require.Equal(t, []string{"name"}, m.Proto().GetPaths())
}

func TestString(t *testing.T) {
	m := FromPaths("name", "display_name")
	require.Contains(t, m.String(), "name")
	require.Contains(t, m.String(), "display_name")
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		paths   []string
		wantErr bool
	}{
		{"valid single", []string{"name"}, false},
		{"valid nested", []string{"author.name"}, false},
		{"valid multiple", []string{"name", "display_name", "shelf_type"}, false},
		{"invalid field", []string{"nonexistent"}, true},
		{"invalid nested", []string{"author.nonexistent"}, true},
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

func TestApply(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
		CreateTime:  timestamppb.Now(),
	}
	fm := FromPaths("display_name")
	err := fm.Validate(shelf)
	require.NoError(t, err)
	fm.Apply(shelf)
	require.NoError(t, err)
	require.Equal(t, "Fiction", shelf.DisplayName)
	require.Empty(t, shelf.Name)
	require.Nil(t, shelf.CreateTime)
	require.Equal(t, pb.ShelfType_SHELF_TYPE_UNSPECIFIED, shelf.ShelfType)
}

func TestApply_Nested(t *testing.T) {
	shelf := &pb.Shelf{
		Name:   "shelves/1",
		Author: &pb.Author{Name: "authors/1", DisplayName: "John"},
	}
	fm := FromPaths("author.name")
	err := fm.Validate(shelf)
	require.NoError(t, err)
	fm.Apply(shelf)
	require.NoError(t, err)
	require.Empty(t, shelf.Name)
	require.Equal(t, "authors/1", shelf.Author.GetName())
	require.Empty(t, shelf.Author.GetDisplayName())
}

func TestApplyInverse(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
	}
	fm := FromPaths("display_name")
	err := fm.Validate(shelf)
	require.NoError(t, err)
	fm.ApplyInverse(shelf)
	require.Empty(t, shelf.DisplayName)
	require.Equal(t, "shelves/1", shelf.Name)
	require.Equal(t, pb.ShelfType_SHELF_TYPE_FICTION, shelf.ShelfType)
}

func TestApplyAny(t *testing.T) {
	shelf := &pb.Shelf{Name: "shelves/1", DisplayName: "Fiction"}
	anyMsg, err := anypb.New(shelf)
	require.NoError(t, err)

	err = FromPaths("name").ApplyAny(anyMsg)
	require.NoError(t, err)

	var result pb.Shelf
	require.NoError(t, anyMsg.UnmarshalTo(&result))
	require.Equal(t, "shelves/1", result.Name)
	require.Empty(t, result.DisplayName)
}

func TestApplyAny_UnknownType(t *testing.T) {
	anyMsg := &anypb.Any{TypeUrl: "type.googleapis.com/unknown.Type"}
	err := FromPaths("name").ApplyAny(anyMsg)
	require.Error(t, err)
}

func TestFromMessage_AllFields(t *testing.T) {
	paths := FromMessage(&pb.Shelf{}).GetPaths()
	require.ElementsMatch(t, []string{
		"name",
		"display_name",
		"create_time",
		"update_time",
		"shelf_type",
		"books",
		"author.name",
		"author.display_name",
		"author.create_time",
		"author.update_time",
	}, paths)
}

func TestFromMessage_WellKnownTypesAsLeaves(t *testing.T) {
	paths := FromMessage(&pb.Shelf{}).GetPaths()
	require.Contains(t, paths, "create_time")
	require.NotContains(t, paths, "create_time.seconds")
	require.NotContains(t, paths, "create_time.nanos")
}

func TestFromMessage_OnlySet(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
	}
	paths := FromMessage(shelf, WithOnlySet()).GetPaths()
	require.ElementsMatch(t, []string{"name", "display_name"}, paths)
}

func TestFromMessage_OnlySet_Nested(t *testing.T) {
	shelf := &pb.Shelf{
		Name:   "shelves/1",
		Author: &pb.Author{Name: "authors/1", DisplayName: "John"},
	}
	paths := FromMessage(shelf, WithOnlySet()).GetPaths()
	require.ElementsMatch(t, []string{"name", "author.name", "author.display_name"}, paths)
}

func TestFromMessage_OnlySet_EmptyNested(t *testing.T) {
	shelf := &pb.Shelf{
		Name:   "shelves/1",
		Author: &pb.Author{},
	}
	paths := FromMessage(shelf, WithOnlySet()).GetPaths()
	require.ElementsMatch(t, []string{"name", "author"}, paths)
}

func TestFromMessage_OnlySet_RepeatedField(t *testing.T) {
	shelf := &pb.Shelf{
		Books: []*pb.Book{{Title: "Book 1"}},
	}
	paths := FromMessage(shelf, WithOnlySet()).GetPaths()
	require.ElementsMatch(t, []string{"books"}, paths)
}

func TestFromMessage_OnlySet_Timestamp(t *testing.T) {
	shelf := &pb.Shelf{
		Name:       "shelves/1",
		CreateTime: timestamppb.Now(),
	}
	paths := FromMessage(shelf, WithOnlySet()).GetPaths()
	require.ElementsMatch(t, []string{"name", "create_time"}, paths)
}

func TestValidate_Wildcard(t *testing.T) {
	tests := []struct {
		name    string
		paths   []string
		wantErr bool
	}{
		{"wildcard alone", []string{"*"}, false},
		{"wildcard with other paths", []string{"*", "name"}, true},
		{"wildcard with other paths reversed", []string{"name", "*"}, true},
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

func TestApply_Wildcard(t *testing.T) {
	shelf := &pb.Shelf{
		Name:        "shelves/1",
		DisplayName: "Fiction",
		ShelfType:   pb.ShelfType_SHELF_TYPE_FICTION,
		CreateTime:  timestamppb.Now(),
	}
	fm := FromPaths("*")
	err := fm.Validate(shelf)
	require.NoError(t, err)
	fm.Apply(shelf)

	require.Equal(t, "shelves/1", shelf.Name)
	require.Equal(t, "Fiction", shelf.DisplayName)
	require.Equal(t, pb.ShelfType_SHELF_TYPE_FICTION, shelf.ShelfType)
	require.NotNil(t, shelf.CreateTime)
}

func TestApply_RepeatedNested(t *testing.T) {
	shelf := &pb.Shelf{
		Name: "shelves/1",
		Books: []*pb.Book{
			{Title: "Book 1", Author: "authors/1"},
			{Title: "Book 2", Author: "authors/2"},
		},
	}
	fm := FromPaths("books.author")
	err := fm.Validate(shelf)
	require.NoError(t, err)
	fm.Apply(shelf)

	require.Empty(t, shelf.Name)
	require.Len(t, shelf.Books, 2)
	require.Empty(t, shelf.Books[0].Title)
	require.Equal(t, "authors/1", shelf.Books[0].Author)
	require.Empty(t, shelf.Books[1].Title)
	require.Equal(t, "authors/2", shelf.Books[1].Author)
}

func TestUpdate(t *testing.T) {
	tests := []struct {
		name     string
		dest     *pb.Shelf
		src      *pb.Shelf
		paths    []string
		expected *pb.Shelf
	}{
		{
			name:     "single field",
			dest:     &pb.Shelf{Name: "shelves/1", DisplayName: "Old"},
			src:      &pb.Shelf{DisplayName: "New"},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "shelves/1", DisplayName: "New"},
		},
		{
			name:     "multiple fields",
			dest:     &pb.Shelf{Name: "shelves/1", DisplayName: "Old", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION},
			src:      &pb.Shelf{DisplayName: "New", ShelfType: pb.ShelfType_SHELF_TYPE_REFERENCE},
			paths:    []string{"display_name", "shelf_type"},
			expected: &pb.Shelf{Name: "shelves/1", DisplayName: "New", ShelfType: pb.ShelfType_SHELF_TYPE_REFERENCE},
		},
		{
			name:     "nested message field",
			dest:     &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1", DisplayName: "Old Author"}},
			src:      &pb.Shelf{Author: &pb.Author{DisplayName: "New Author"}},
			paths:    []string{"author.display_name"},
			expected: &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1", DisplayName: "New Author"}},
		},
		{
			name:     "entire nested message",
			dest:     &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1", DisplayName: "Old"}},
			src:      &pb.Shelf{Author: &pb.Author{Name: "authors/2", DisplayName: "New"}},
			paths:    []string{"author"},
			expected: &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/2", DisplayName: "New"}},
		},
		{
			name:     "repeated field copied by reference",
			dest:     &pb.Shelf{Name: "shelves/1", Books: []*pb.Book{{Title: "Old"}}},
			src:      &pb.Shelf{Books: []*pb.Book{{Title: "New1"}, {Title: "New2"}}},
			paths:    []string{"books"},
			expected: &pb.Shelf{Name: "shelves/1", Books: []*pb.Book{{Title: "New1"}, {Title: "New2"}}},
		},
		{
			name:     "clear field with zero value",
			dest:     &pb.Shelf{Name: "shelves/1", DisplayName: "Fiction"},
			src:      &pb.Shelf{DisplayName: ""},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "shelves/1", DisplayName: ""},
		},
		{
			name:     "clear nested message",
			dest:     &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1"}},
			src:      &pb.Shelf{Author: nil},
			paths:    []string{"author"},
			expected: &pb.Shelf{Name: "shelves/1", Author: nil},
		},
		{
			name:     "timestamp field",
			dest:     &pb.Shelf{Name: "shelves/1", CreateTime: timestamppb.New(time.Unix(1000, 0))},
			src:      &pb.Shelf{CreateTime: timestamppb.New(time.Unix(2000, 0))},
			paths:    []string{"create_time"},
			expected: &pb.Shelf{Name: "shelves/1", CreateTime: timestamppb.New(time.Unix(2000, 0))},
		},
		{
			name:     "wildcard replaces all fields",
			dest:     &pb.Shelf{Name: "shelves/1", DisplayName: "Old", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION},
			src:      &pb.Shelf{Name: "shelves/2", DisplayName: "New"},
			paths:    []string{"*"},
			expected: &pb.Shelf{Name: "shelves/2", DisplayName: "New", ShelfType: pb.ShelfType_SHELF_TYPE_UNSPECIFIED},
		},
		{
			name:     "field not in mask unchanged",
			dest:     &pb.Shelf{Name: "shelves/1", DisplayName: "Old", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION},
			src:      &pb.Shelf{Name: "shelves/2", DisplayName: "New", ShelfType: pb.ShelfType_SHELF_TYPE_REFERENCE},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "shelves/1", DisplayName: "New", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION},
		},
		{
			name:     "enum field",
			dest:     &pb.Shelf{Name: "shelves/1", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION},
			src:      &pb.Shelf{ShelfType: pb.ShelfType_SHELF_TYPE_NON_FICTION},
			paths:    []string{"shelf_type"},
			expected: &pb.Shelf{Name: "shelves/1", ShelfType: pb.ShelfType_SHELF_TYPE_NON_FICTION},
		},
		{
			name:     "set nested when dest is nil",
			dest:     &pb.Shelf{Name: "shelves/1", Author: nil},
			src:      &pb.Shelf{Author: &pb.Author{Name: "authors/1", DisplayName: "New"}},
			paths:    []string{"author"},
			expected: &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1", DisplayName: "New"}},
		},
		{
			name:     "update nested field when dest nested is nil",
			dest:     &pb.Shelf{Name: "shelves/1", Author: nil},
			src:      &pb.Shelf{Author: &pb.Author{DisplayName: "New"}},
			paths:    []string{"author.display_name"},
			expected: &pb.Shelf{Name: "shelves/1", Author: &pb.Author{DisplayName: "New"}},
		},
		{
			name:     "multiple nested fields",
			dest:     &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/1", DisplayName: "Old"}},
			src:      &pb.Shelf{Author: &pb.Author{Name: "authors/2", DisplayName: "New"}},
			paths:    []string{"author.name", "author.display_name"},
			expected: &pb.Shelf{Name: "shelves/1", Author: &pb.Author{Name: "authors/2", DisplayName: "New"}},
		},
		{
			name:     "clear repeated field",
			dest:     &pb.Shelf{Name: "shelves/1", Books: []*pb.Book{{Title: "Book1"}}},
			src:      &pb.Shelf{Books: nil},
			paths:    []string{"books"},
			expected: &pb.Shelf{Name: "shelves/1", Books: nil},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fm := FromPaths(tc.paths...)
			fm.Update(tc.dest, tc.src)
			require.Equal(t, tc.expected.Name, tc.dest.Name)
			require.Equal(t, tc.expected.DisplayName, tc.dest.DisplayName)
			require.Equal(t, tc.expected.ShelfType, tc.dest.ShelfType)
			if tc.expected.Author == nil {
				require.Nil(t, tc.dest.Author)
			} else {
				require.NotNil(t, tc.dest.Author)
				require.Equal(t, tc.expected.Author.Name, tc.dest.Author.Name)
				require.Equal(t, tc.expected.Author.DisplayName, tc.dest.Author.DisplayName)
			}
			if tc.expected.CreateTime == nil {
				require.Nil(t, tc.dest.CreateTime)
			} else {
				require.NotNil(t, tc.dest.CreateTime)
				require.Equal(t, tc.expected.CreateTime.AsTime(), tc.dest.CreateTime.AsTime())
			}
			require.Equal(t, len(tc.expected.Books), len(tc.dest.Books))
			for i := range tc.expected.Books {
				require.Equal(t, tc.expected.Books[i].Title, tc.dest.Books[i].Title)
			}
		})
	}
}

func TestUpdate_EmptyMask_CopiesNonZeroOnly(t *testing.T) {
	dest := &pb.Shelf{Name: "shelves/1", DisplayName: "Old", ShelfType: pb.ShelfType_SHELF_TYPE_FICTION}
	src := &pb.Shelf{DisplayName: "New"}

	FromPaths().Update(dest, src)

	require.Equal(t, "shelves/1", dest.Name)
	require.Equal(t, "New", dest.DisplayName)
	require.Equal(t, pb.ShelfType_SHELF_TYPE_FICTION, dest.ShelfType)
}
