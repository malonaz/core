package pbfieldmask

import (
	"testing"
	"time"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestNew(t *testing.T) {
	fieldMask := &fieldmaskpb.FieldMask{Paths: []string{"display_name", "name"}}
	mask := New(fieldMask)
	require.ElementsMatch(t, []string{"display_name", "name"}, mask.GetPaths())
}

func TestNew_Normalization(t *testing.T) {
	fieldMask := &fieldmaskpb.FieldMask{Paths: []string{"metadata", "metadata.capacity"}}
	mask := New(fieldMask)
	require.Equal(t, []string{"metadata"}, mask.GetPaths())
}

func TestFromPaths(t *testing.T) {
	mask := FromPaths("name", "display_name", "genre")
	require.ElementsMatch(t, []string{"name", "display_name", "genre"}, mask.GetPaths())
}

func TestFromString(t *testing.T) {
	mask := FromString("name,display_name,metadata.capacity")
	require.ElementsMatch(t, []string{"name", "display_name", "metadata.capacity"}, mask.GetPaths())
}

func TestFromString_Empty(t *testing.T) {
	mask := FromString("")
	require.Equal(t, []string{""}, mask.GetPaths())
}

func TestProto(t *testing.T) {
	mask := FromPaths("name")
	require.NotNil(t, mask.Proto())
	require.Equal(t, []string{"name"}, mask.Proto().GetPaths())
}

func TestGetPaths(t *testing.T) {
	mask := FromPaths("name", "genre")
	require.ElementsMatch(t, []string{"name", "genre"}, mask.GetPaths())
}

func TestString(t *testing.T) {
	mask := FromPaths("name", "display_name")
	s := mask.String()
	require.Contains(t, s, "name")
	require.Contains(t, s, "display_name")
}

func TestIsWildcardPath(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		expected bool
	}{
		{"wildcard", []string{"*"}, true},
		{"single field", []string{"name"}, false},
		{"multiple fields", []string{"name", "genre"}, false},
		{"wildcard among others", []string{"*", "name"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FromPaths(tc.paths...).IsWildcardPath())
		})
	}
}

func TestWithParent(t *testing.T) {
	mask := FromPaths("name", "display_name").WithParent("shelf")
	require.ElementsMatch(t, []string{"shelf.name", "shelf.display_name"}, mask.GetPaths())
}

func TestWithParent_Wildcard_Panics(t *testing.T) {
	require.Panics(t, func() {
		FromPaths("*").WithParent("shelf")
	})
}

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		query    string
		expected bool
	}{
		{"exact match", []string{"name"}, "name", true},
		{"no match", []string{"name"}, "genre", false},
		{"parent contains child", []string{"metadata"}, "metadata.capacity", true},
		{"child contains parent", []string{"metadata.capacity"}, "metadata", true},
		{"wildcard matches anything", []string{"*"}, "metadata.capacity", true},
		{"sibling no match", []string{"metadata.capacity"}, "metadata.dummy", false},
		{"partial name no match", []string{"name"}, "names", false},
		{"multiple paths match second", []string{"name", "genre"}, "genre", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FromPaths(tc.paths...).Contains(tc.query))
		})
	}
}

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
		{"valid wildcard", []string{"*"}, false},
		{"invalid field", []string{"nonexistent"}, true},
		{"invalid nested", []string{"metadata.nonexistent"}, true},
		{"invalid deep", []string{"metadata.capacity.foo"}, true},
		{"wildcard with others", []string{"*", "name"}, true},
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

func TestMustValidate(t *testing.T) {
	mask := FromPaths("name", "genre").MustValidate(&pb.Shelf{})
	require.ElementsMatch(t, []string{"name", "genre"}, mask.GetPaths())
}

func TestMustValidate_Panics(t *testing.T) {
	require.Panics(t, func() {
		FromPaths("nonexistent").MustValidate(&pb.Shelf{})
	})
}

func TestApply(t *testing.T) {
	tests := []struct {
		name     string
		message  proto.Message
		paths    []string
		expected proto.Message
	}{
		{
			name: "single field",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
				CreateTime:  timestamppb.Now(),
			},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{DisplayName: "Fiction"},
		},
		{
			name: "multiple fields",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
				ExternalId:  "ext-1",
			},
			paths:    []string{"display_name", "genre"},
			expected: &pb.Shelf{DisplayName: "Fiction", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
		},
		{
			name: "nested subfield prunes sibling nested fields",
			message: &pb.Shelf{
				Name: "organizations/1/shelves/1",
				Metadata: &pb.ShelfMetadata{
					Capacity: 100,
					Dummy:    "should be pruned",
					Notes:    []*pb.ShelfNote{{Content: "should be pruned"}},
				},
			},
			paths:    []string{"metadata.capacity"},
			expected: &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 100}},
		},
		{
			name: "deeply nested subfield prunes sibling nested fields on author",
			message: &pb.Author{
				Name:        "organizations/1/authors/1",
				DisplayName: "John",
				Metadata: &pb.AuthorMetadata{
					Country:        "US",
					EmailAddresses: []string{"a@b.com"},
					PhoneNumbers:   []string{"+1234567890"},
				},
			},
			paths:    []string{"metadata.country"},
			expected: &pb.Author{Metadata: &pb.AuthorMetadata{Country: "US"}},
		},
		{
			name: "map field only",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Labels:      map[string]string{"env": "prod", "team": "core"},
			},
			paths:    []string{"labels"},
			expected: &pb.Shelf{Labels: map[string]string{"env": "prod", "team": "core"}},
		},
		{
			name: "repeated scalar field only",
			message: &pb.Author{
				Name:           "organizations/1/authors/1",
				DisplayName:    "John",
				EmailAddresses: []string{"a@b.com", "c@d.com"},
			},
			paths:    []string{"email_addresses"},
			expected: &pb.Author{EmailAddresses: []string{"a@b.com", "c@d.com"}},
		},
		{
			name: "repeated message subfield",
			message: &pb.ShelfMetadata{
				Capacity: 100,
				Dummy:    "test",
				Notes:    []*pb.ShelfNote{{Content: "note1"}, {Content: "note2"}},
			},
			paths:    []string{"notes"},
			expected: &pb.ShelfMetadata{Notes: []*pb.ShelfNote{{Content: "note1"}, {Content: "note2"}}},
		},
		{
			name: "wildcard retains all",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
				CreateTime:  timestamppb.New(time.Unix(1000, 0)),
			},
			paths: []string{"*"},
			expected: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
				CreateTime:  timestamppb.New(time.Unix(1000, 0)),
			},
		},
		{
			name: "entire nested message retains all subfields",
			message: &pb.Shelf{
				Name: "organizations/1/shelves/1",
				Metadata: &pb.ShelfMetadata{
					Capacity: 100,
					Dummy:    "keep",
					Notes:    []*pb.ShelfNote{{Content: "keep"}},
				},
			},
			paths: []string{"metadata"},
			expected: &pb.Shelf{
				Metadata: &pb.ShelfMetadata{
					Capacity: 100,
					Dummy:    "keep",
					Notes:    []*pb.ShelfNote{{Content: "keep"}},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			FromPaths(tc.paths...).Apply(tc.message)
			grpcrequire.Equal(t, tc.expected, tc.message)
		})
	}
}

func TestApplyInverse(t *testing.T) {
	tests := []struct {
		name     string
		message  proto.Message
		paths    []string
		expected proto.Message
	}{
		{
			name: "prune single field",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
			},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
		},
		{
			name: "prune nested subfield keeps siblings",
			message: &pb.Shelf{
				Name: "organizations/1/shelves/1",
				Metadata: &pb.ShelfMetadata{
					Capacity: 100,
					Dummy:    "keep",
				},
			},
			paths:    []string{"metadata.capacity"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Dummy: "keep"}},
		},
		{
			name: "wildcard resets entire message",
			message: &pb.Shelf{
				Name:        "organizations/1/shelves/1",
				DisplayName: "Fiction",
				Genre:       pb.ShelfGenre_SHELF_GENRE_FICTION,
			},
			paths:    []string{"*"},
			expected: &pb.Shelf{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			FromPaths(tc.paths...).ApplyInverse(tc.message)
			grpcrequire.Equal(t, tc.expected, tc.message)
		})
	}
}

func TestApplyAny(t *testing.T) {
	shelf := &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Fiction"}
	anyMessage, err := anypb.New(shelf)
	require.NoError(t, err)

	require.NoError(t, FromPaths("name").ApplyAny(anyMessage))

	var result pb.Shelf
	require.NoError(t, anyMessage.UnmarshalTo(&result))
	grpcrequire.Equal(t, &pb.Shelf{Name: "organizations/1/shelves/1"}, &result)
}

func TestApplyAny_UnknownType(t *testing.T) {
	anyMessage := &anypb.Any{TypeUrl: "type.googleapis.com/unknown.Type"}
	require.Error(t, FromPaths("name").ApplyAny(anyMessage))
}

func TestFromMessage_AllFields_Shelf(t *testing.T) {
	paths := FromMessage(&pb.Shelf{}).GetPaths()
	require.ElementsMatch(t, []string{
		"name",
		"create_time",
		"update_time",
		"delete_time",
		"display_name",
		"genre",
		"external_id",
		"correlation_id_2",
		"labels",
		"metadata.capacity",
		"metadata.dummy",
		"metadata.notes",
		"metadata.author_to_note",
	}, paths)
}

func TestFromMessage_AllFields_Author(t *testing.T) {
	paths := FromMessage(&pb.Author{}).GetPaths()
	require.ElementsMatch(t, []string{
		"name",
		"create_time",
		"update_time",
		"delete_time",
		"display_name",
		"biography",
		"email_address",
		"phone_number",
		"email_addresses",
		"phone_numbers",
		"labels",
		"etag",
		"metadata.country",
		"metadata.email_addresses",
		"metadata.phone_numbers",
	}, paths)
}

func TestFromMessage_AllFields_Book(t *testing.T) {
	paths := FromMessage(&pb.Book{}).GetPaths()
	require.ElementsMatch(t, []string{
		"name",
		"create_time",
		"update_time",
		"title",
		"author",
		"isbn",
		"publication_year",
		"page_count",
		"labels",
		"etag",
		"metadata.summary",
		"metadata.language",
		"metadata.phone_number",
	}, paths)
}

func TestFromMessage_WellKnownTypesAsLeaves(t *testing.T) {
	paths := FromMessage(&pb.Shelf{}).GetPaths()
	require.Contains(t, paths, "create_time")
	require.NotContains(t, paths, "create_time.seconds")
	require.NotContains(t, paths, "create_time.nanos")
}

func TestFromMessage_OnlySet(t *testing.T) {
	tests := []struct {
		name     string
		message  proto.Message
		expected []string
	}{
		{
			name:     "scalar fields",
			message:  &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Fiction"},
			expected: []string{"name", "display_name"},
		},
		{
			name:     "enum field",
			message:  &pb.Shelf{Name: "organizations/1/shelves/1", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
			expected: []string{"name", "genre"},
		},
		{
			name: "nested with populated subfields",
			message: &pb.Shelf{
				Name:     "organizations/1/shelves/1",
				Metadata: &pb.ShelfMetadata{Capacity: 100, Dummy: "test"},
			},
			expected: []string{"name", "metadata.capacity", "metadata.dummy"},
		},
		{
			name:     "empty nested message",
			message:  &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{}},
			expected: []string{"name", "metadata"},
		},
		{
			name:     "repeated scalar",
			message:  &pb.Author{EmailAddresses: []string{"a@b.com"}},
			expected: []string{"email_addresses"},
		},
		{
			name:     "map field",
			message:  &pb.Shelf{Labels: map[string]string{"env": "prod"}},
			expected: []string{"labels"},
		},
		{
			name:     "timestamp",
			message:  &pb.Shelf{Name: "organizations/1/shelves/1", CreateTime: timestamppb.Now()},
			expected: []string{"name", "create_time"},
		},
		{
			name:     "no fields set",
			message:  &pb.Shelf{},
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			paths := FromMessage(tc.message, WithOnlySet()).GetPaths()
			if tc.expected == nil {
				require.Empty(t, paths)
			} else {
				require.ElementsMatch(t, tc.expected, paths)
			}
		})
	}
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
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Old"},
			src:      &pb.Shelf{DisplayName: "New"},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "New"},
		},
		{
			name:     "multiple fields",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Old", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
			src:      &pb.Shelf{DisplayName: "New", Genre: pb.ShelfGenre_SHELF_GENRE_HISTORY},
			paths:    []string{"display_name", "genre"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "New", Genre: pb.ShelfGenre_SHELF_GENRE_HISTORY},
		},
		{
			name:     "nested subfield preserves siblings",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 50, Dummy: "old"}},
			src:      &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 100}},
			paths:    []string{"metadata.capacity"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 100, Dummy: "old"}},
		},
		{
			name:     "entire nested message replaces all subfields",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 50, Dummy: "old"}},
			src:      &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 200}},
			paths:    []string{"metadata"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 200}},
		},
		{
			name:     "clear field with zero value",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Fiction"},
			src:      &pb.Shelf{},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1"},
		},
		{
			name:     "clear nested message",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 50}},
			src:      &pb.Shelf{},
			paths:    []string{"metadata"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1"},
		},
		{
			name:     "set nested when dest is nil",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1"},
			src:      &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 100, Dummy: "new"}},
			paths:    []string{"metadata"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 100, Dummy: "new"}},
		},
		{
			name:     "update nested subfield when dest nested is nil",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1"},
			src:      &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 100}},
			paths:    []string{"metadata.capacity"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 100}},
		},
		{
			name:     "multiple nested subfields",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 50, Dummy: "old"}},
			src:      &pb.Shelf{Metadata: &pb.ShelfMetadata{Capacity: 200, Dummy: "new"}},
			paths:    []string{"metadata.capacity", "metadata.dummy"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Metadata: &pb.ShelfMetadata{Capacity: 200, Dummy: "new"}},
		},
		{
			name:     "timestamp field",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", CreateTime: timestamppb.New(time.Unix(1000, 0))},
			src:      &pb.Shelf{CreateTime: timestamppb.New(time.Unix(2000, 0))},
			paths:    []string{"create_time"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", CreateTime: timestamppb.New(time.Unix(2000, 0))},
		},
		{
			name:     "enum field",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
			src:      &pb.Shelf{Genre: pb.ShelfGenre_SHELF_GENRE_NON_FICTION},
			paths:    []string{"genre"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Genre: pb.ShelfGenre_SHELF_GENRE_NON_FICTION},
		},
		{
			name:     "map field",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"old": "val"}},
			src:      &pb.Shelf{Labels: map[string]string{"new": "val"}},
			paths:    []string{"labels"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"new": "val"}},
		},
		{
			name:     "clear map field",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"k": "v"}},
			src:      &pb.Shelf{},
			paths:    []string{"labels"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1"},
		},
		{
			name:     "wildcard replaces all",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Old", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
			src:      &pb.Shelf{Name: "organizations/1/shelves/2", DisplayName: "New"},
			paths:    []string{"*"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/2", DisplayName: "New"},
		},
		{
			name:     "field not in mask unchanged",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Old", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
			src:      &pb.Shelf{Name: "organizations/1/shelves/2", DisplayName: "New", Genre: pb.ShelfGenre_SHELF_GENRE_HISTORY},
			paths:    []string{"display_name"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "New", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			FromPaths(tc.paths...).Update(tc.dest, tc.src)
			grpcrequire.Equal(t, tc.expected, tc.dest)
		})
	}
}

func TestUpdate_EmptyMask_CopiesNonZeroOnly(t *testing.T) {
	dest := &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "Old", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION}
	src := &pb.Shelf{DisplayName: "New"}
	FromPaths().Update(dest, src)
	expected := &pb.Shelf{Name: "organizations/1/shelves/1", DisplayName: "New", Genre: pb.ShelfGenre_SHELF_GENRE_FICTION}
	grpcrequire.Equal(t, expected, dest)
}
