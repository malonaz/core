// file: go/pbutil/pbfieldmask/pbfieldmask_test.go
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
		"duration",
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
		"duration",
		"labels",
		"etag",
		"metadata.summary",
		"metadata.language",
		"metadata.phone_number",
		"metadata.duration",
		"shelf_external_id",
		"shelf_genre",
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

func TestSplitPathSegments(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected []string
	}{
		{"simple", "name", []string{"name"}},
		{"dotted", "metadata.capacity", []string{"metadata", "capacity"}},
		{"backtick key", "labels.`my key`", []string{"labels", "my key"}},
		{"backtick key with dots", "labels.`my.dotted.key`", []string{"labels", "my.dotted.key"}},
		{"backtick then more", "labels.`key`.sub", []string{"labels", "key", "sub"}},
		{"multiple dots", "a.b.c.d", []string{"a", "b", "c", "d"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, splitPathSegments(tc.path))
		})
	}
}
