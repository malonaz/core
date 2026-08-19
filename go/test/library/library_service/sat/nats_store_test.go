package sat

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/go/aip"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// TestNatsStore covers the buffer satEnvironment.NatsStore holds over the subjects declared in
// sat_test.go. Unlike the processors in nats_events_test.go it needs no consumer of its own: the
// store subscribed before the service booted, so the event is already there to be selected on.
func TestNatsStore(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	organizationResourceName := &librarypb.OrganizationResourceName{}
	require.NoError(t, organizationResourceName.UnmarshalString(organizationParent))

	genre := librarypb.ShelfGenre_SHELF_GENRE_FICTION
	shelf := createTestShelf(t, organizationParent, "Nats Store Shelf", genre)

	subject := librarypb.GetShelfStream().GetCreatedSubject().
		WithOrganization(organizationResourceName.Organization).
		WithGenre(genre).
		MustGet().
		Name()
	event := satEnvironment.NatsStore.Require(t, subject)

	published, err := aip.ParseEventResource[*librarypb.Shelf](event)
	require.NoError(t, err)
	require.Equal(t, shelf.Name, published.Name)
	require.Equal(t, "Nats Store Shelf", published.DisplayName)
	require.Equal(t, genre, published.Genre)

	// Require consumed it, so the organization's subjects are empty again.
	require.Empty(t, satEnvironment.NatsStore.Messages(subject))
}
