package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func createTestShelf(t *testing.T, parent, displayName string, genre librarypb.ShelfGenre) *librarypb.Shelf {
	t.Helper()
	createShelfRequest := &libraryservicepb.CreateShelfRequest{
		Parent: parent,
		Shelf: &librarypb.Shelf{
			DisplayName: displayName,
			Genre:       genre,
			Metadata:    &librarypb.ShelfMetadata{Capacity: 100},
		},
	}
	shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
	require.NoError(t, err)
	return shelf
}

func TestShelfCreate(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Science Fiction Classics",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION,
				Metadata:    &librarypb.ShelfMetadata{Capacity: 100},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)
		require.NotEmpty(t, createdShelf.Name)
		require.NotNil(t, createdShelf.CreateTime)
		require.NotNil(t, createdShelf.UpdateTime)
		require.Nil(t, createdShelf.DeleteTime)
		require.Equal(t, "Science Fiction Classics", createdShelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, createdShelf.Genre)
		require.Equal(t, int32(100), createdShelf.Metadata.Capacity)
	})

	t.Run("Protovalidation_GenreUnspecified", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Bad Genre Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED,
				Metadata:    &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingDisplayName", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				Genre:    librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata: &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Shelf: &librarypb.Shelf{
				DisplayName: "No Parent Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestShelfUpdate(t *testing.T) {
	t.Run("AllowedFields", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        shelf.Name,
				DisplayName: "Updated Shelf Name",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "genre"}},
		}
		updatedShelf, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated Shelf Name", updatedShelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, updatedShelf.Genre)
	})

	t.Run("MetadataPartialUpdate", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Metadata Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:     shelf.Name,
				Metadata: &librarypb.ShelfMetadata{Capacity: 250},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.capacity"}},
		}
		updatedShelf, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)
		require.Equal(t, int32(250), updatedShelf.Metadata.Capacity)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Unauthorized Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name: shelf.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UpdateTimeChanges", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Update Time Shelf", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        shelf.Name,
				DisplayName: "Time Check Shelf",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		updatedShelf, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)
		require.True(t, updatedShelf.UpdateTime.AsTime().After(shelf.UpdateTime.AsTime()) ||
			updatedShelf.UpdateTime.AsTime().Equal(shelf.UpdateTime.AsTime()))
	})
}

func TestShelfDelete(t *testing.T) {
	t.Run("SoftDelete", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Delete Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: shelf.Name,
		}
		deletedShelf, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedShelf.DeleteTime)

		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(ctx, getShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, gotShelf.DeleteTime)
	})

	t.Run("SoftDeletedHiddenFromList", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Hidden Deleted Shelf 99887", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: shelf.Name,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: `display_name = "Hidden Deleted Shelf 99887"`,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Empty(t, listShelvesResponse.Shelves)
	})

	t.Run("ShowDeletedRevealsDeleted", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Show Deleted Shelf 44556", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: shelf.Name,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:      organizationParent,
			Filter:      `display_name = "Show Deleted Shelf 44556"`,
			ShowDeleted: true,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.NotNil(t, listShelvesResponse.Shelves[0].DeleteTime)
	})

	t.Run("AllowMissing does not return error on already deleted resource", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		shelf := createTestShelf(t, organizationParent, "Hidden Deleted Shelf 345319", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION)

		// Initial delete.
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: shelf.Name,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		// Another delete on an already deleted resource.
		deleteShelfRequest = &libraryservicepb.DeleteShelfRequest{
			Name: shelf.Name,
		}
		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		// Bow we set allow missing = true.
		deleteShelfRequest = &libraryservicepb.DeleteShelfRequest{
			Name:         shelf.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		// Get the shelf.
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(ctx, getShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, gotShelf.DeleteTime)
	})

	t.Run("AllowMissing still throws not found error on soft deletable resource if it does not exist", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name:         organizationParent + "/shelves/nonexistent-shelf",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: organizationParent + "/shelves/nonexistent-shelf-err",
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestShelfList(t *testing.T) {
	t.Run("FilterByGenreEnum", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createTestShelf(t, organizationParent, "History Shelf for Filter", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: fmt.Sprintf(`genre = %s`, librarypb.ShelfGenre_SHELF_GENRE_HISTORY.String()),
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listShelvesResponse.Shelves), 1)
		for _, shelf := range listShelvesResponse.Shelves {
			require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, shelf.Genre)
		}
	})

	t.Run("FilterByMetadataCapacity", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Large Capacity Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:    &librarypb.ShelfMetadata{Capacity: 9999},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: `metadata.capacity >= 9999`,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listShelvesResponse.Shelves), 1)
	})

	t.Run("OrderByAllowed_DisplayName", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:  organizationParent,
			OrderBy: "display_name asc",
		}
		_, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByNotAllowed", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:  organizationParent,
			OrderBy: "genre asc",
		}
		_, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("FilterWithLabels", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Labeled Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Labels:      map[string]string{"floor": "unique-floor-3"},
				Metadata:    &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: `labels.floor = "unique-floor-3"`,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
	})
}
