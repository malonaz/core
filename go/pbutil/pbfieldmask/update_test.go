package pbfieldmask

import (
	"testing"
	"time"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/test/library/v1"
)

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
			name:     "map field full replacement",
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
			name:     "map field single key update",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "b": "2"}},
			src:      &pb.Shelf{Labels: map[string]string{"a": "updated"}},
			paths:    []string{"labels.a"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "updated", "b": "2"}},
		},
		{
			name:     "map field add new key",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1"}},
			src:      &pb.Shelf{Labels: map[string]string{"b": "2"}},
			paths:    []string{"labels.b"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "b": "2"}},
		},
		{
			name:     "map field remove key when absent in src",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "b": "2"}},
			src:      &pb.Shelf{Labels: map[string]string{}},
			paths:    []string{"labels.a"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"b": "2"}},
		},
		{
			name:     "map field remove key when src has no map",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "b": "2"}},
			src:      &pb.Shelf{},
			paths:    []string{"labels.a"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"b": "2"}},
		},
		{
			name:     "map field multiple key updates",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "b": "2", "c": "3"}},
			src:      &pb.Shelf{Labels: map[string]string{"a": "x", "c": "z"}},
			paths:    []string{"labels.a", "labels.c"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "x", "b": "2", "c": "z"}},
		},
		{
			name:     "map field backtick quoted key",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1"}},
			src:      &pb.Shelf{Labels: map[string]string{"my key": "val"}},
			paths:    []string{"labels.`my key`"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "my key": "val"}},
		},
		{
			name:     "map field backtick quoted key with dots",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1"}},
			src:      &pb.Shelf{Labels: map[string]string{"my.dotted.key": "val"}},
			paths:    []string{"labels.`my.dotted.key`"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1", "my.dotted.key": "val"}},
		},
		{
			name:     "map field add key to nil dest map",
			dest:     &pb.Shelf{Name: "organizations/1/shelves/1"},
			src:      &pb.Shelf{Labels: map[string]string{"a": "1"}},
			paths:    []string{"labels.a"},
			expected: &pb.Shelf{Name: "organizations/1/shelves/1", Labels: map[string]string{"a": "1"}},
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
