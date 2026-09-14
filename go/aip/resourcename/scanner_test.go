package resourcename

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentScanner(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		input       string
		full        bool
		serviceName string
		segments    []Segment
	}{
		{
			name:     "empty",
			input:    "",
			segments: []Segment{},
		},

		{
			name:     "singleton",
			input:    "singleton",
			segments: []Segment{"singleton"},
		},

		{
			name:     "two segments",
			input:    "shippers/1",
			segments: []Segment{"shippers", "1"},
		},

		{
			name:     "three segments",
			input:    "shippers/1/settings",
			segments: []Segment{"shippers", "1", "settings"},
		},

		{
			name:     "wildcard segment",
			input:    "shippers/1/shipments/-",
			segments: []Segment{"shippers", "1", "shipments", "-"},
		},

		{
			name:     "empty middle segment",
			input:    "shippers//shipments",
			segments: []Segment{"shippers", "", "shipments"},
		},

		{
			name:     "empty end segment",
			input:    "shippers/",
			segments: []Segment{"shippers", ""},
		},

		{
			name:        "full",
			input:       "//library.googleapis.com/publishers/123/books/les-miserables",
			full:        true,
			serviceName: "library.googleapis.com",
			segments:    []Segment{"publishers", "123", "books", "les-miserables"},
		},

		{
			name:        "full without segments",
			input:       "//library.googleapis.com",
			full:        true,
			serviceName: "library.googleapis.com",
			segments:    []Segment{},
		},

		{
			name:        "full without service name",
			input:       "//",
			full:        true,
			serviceName: "",
			segments:    []Segment{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actualSegments := make([]Segment, 0, len(tt.segments))
			var sc Scanner
			sc.Init(tt.input)
			for sc.Scan() {
				actualSegments = append(actualSegments, sc.Segment())
			}
			require.Equal(t, tt.full, sc.Full())
			require.Equal(t, tt.serviceName, sc.ServiceName())
			require.Equal(t, tt.segments, actualSegments)
		})
	}
}

//nolint:gochecknoglobals
var stringSink string

func BenchmarkScanner(b *testing.B) {
	const name = "//library.googleapis.com/publishers/123/books/les-miserables"
	for i := 0; i < b.N; i++ {
		var sc Scanner
		sc.Init(name)
		for sc.Scan() {
			stringSink = sc.Segment().Literal().ResourceID()
		}
	}
}
