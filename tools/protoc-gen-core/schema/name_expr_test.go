package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/tools/protoc-gen-core/resource"
)

func TestResourceNameExpr(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pattern string
		columns []string
		want    string
	}{
		{
			name:    "Collections",
			pattern: "organizations/{organization}/shelves/{shelf}/books/{book}",
			columns: []string{"organization_id", "shelf_id", "book_id"},
			want:    "'organizations/' || child.organization_id || '/shelves/' || child.shelf_id || '/books/' || child.book_id",
		},
		{
			name:    "CollectionUnderSingleton",
			pattern: "organizations/{organization}/contacts/{contact}/activity/events/{event}",
			columns: []string{"organization_id", "contact_id", "event_id"},
			want:    "'organizations/' || child.organization_id || '/contacts/' || child.contact_id || '/activity/events/' || child.event_id",
		},
		{
			name:    "Singleton",
			pattern: "organizations/{organization}/authors/{author}/profile",
			columns: []string{"organization_id", "author_id"},
			want:    "'organizations/' || child.organization_id || '/authors/' || child.author_id || '/profile'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings := make([]ColumnBinding, len(tc.columns))
			for i, column := range tc.columns {
				bindings[i] = ColumnBinding{Column: column}
			}
			require.Equal(t, tc.want, resourceNameExpr(&resource.ParsedPattern{Value: tc.pattern}, bindings, "child"))
		})
	}
}
