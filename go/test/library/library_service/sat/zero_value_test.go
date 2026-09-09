package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

// A proto3 repeated field has no presence: unset and empty arrive identically,
// so an omitted non-nullable list is stored as an empty array, never as NULL.
func TestCreate_OmittedRepeatedFieldIsEmpty(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	created, err := libraryServiceClient.CreateAuthor(ctx, &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{DisplayName: "No Emails Author", EmailAddress: "none@example.com", Metadata: &librarypb.AuthorMetadata{}},
	})
	require.NoError(t, err)
	require.Empty(t, created.EmailAddresses)
	require.Empty(t, getAuthor(t, created.Name).EmailAddresses)

	// Writing the read-back value round-trips.
	created.Biography = "updated"
	updated, err := libraryServiceClient.UpdateAuthor(ctx, &libraryservicepb.UpdateAuthorRequest{
		Author:     created,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
	})
	require.NoError(t, err)
	require.Empty(t, updated.EmailAddresses)
}

// A message-typed field backing a NOT NULL column is declared required
// (codegen enforces it), so omitting it is rejected by protovalidate at the
// boundary — never a storage-layer Internal error.
func TestCreate_OmittedRequiredMessage_InvalidArgument(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author_Metadata", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.CreateAuthor(ctx, &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{DisplayName: "No Metadata", EmailAddress: "no-meta@example.com"},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Shelf_Metadata", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.CreateShelf(ctx, &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf:  &librarypb.Shelf{DisplayName: "No Metadata", Genre: librarypb.ShelfGenre_SHELF_GENRE_FICTION, CorrelationId_2: "x"},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Book_Duration", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "No Duration Author")
		shelf := createTestShelf(t, organizationParent, "No Duration Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		_, err := libraryServiceClient.CreateBook(ctx, &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book:   &librarypb.Book{Title: "No Duration", Author: author.Name, Metadata: &librarypb.BookMetadata{}},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
