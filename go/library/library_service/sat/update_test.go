package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func updateAuthor(t *testing.T, author *librarypb.Author, paths []string) *librarypb.Author {
	t.Helper()
	updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
		Author:     author,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
	require.NoError(t, err)
	return updatedAuthor
}

func updateBook(t *testing.T, book *librarypb.Book, paths []string) *librarypb.Book {
	t.Helper()
	updateBookRequest := &libraryservicepb.UpdateBookRequest{
		Book:       book,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
	require.NoError(t, err)
	return updatedBook
}

func updateShelf(t *testing.T, shelf *librarypb.Shelf, paths []string) *librarypb.Shelf {
	t.Helper()
	updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
		Shelf:      shelf,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedShelf, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
	require.NoError(t, err)
	return updatedShelf
}

func getAuthor(t *testing.T, name string) *librarypb.Author {
	t.Helper()
	getAuthorRequest := &libraryservicepb.GetAuthorRequest{Name: name}
	author, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
	require.NoError(t, err)
	return author
}

func getBook(t *testing.T, name string) *librarypb.Book {
	t.Helper()
	getBookRequest := &libraryservicepb.GetBookRequest{Name: name}
	book, err := libraryServiceClient.GetBook(ctx, getBookRequest)
	require.NoError(t, err)
	return book
}

func getShelf(t *testing.T, name string) *librarypb.Shelf {
	t.Helper()
	getShelfRequest := &libraryservicepb.GetShelfRequest{Name: name}
	shelf, err := libraryServiceClient.GetShelf(ctx, getShelfRequest)
	require.NoError(t, err)
	return shelf
}

func TestUpdate_SingleField(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("StringField", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "SingleField String Original")

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			DisplayName: "SingleField String Updated",
		}, []string{"display_name"})

		require.Equal(t, "SingleField String Updated", updated.DisplayName)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("IntegerField", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SingleField Int Author")
		shelf := createTestShelf(t, organizationParent, "SingleField Int Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "SingleField Int Book")

		updated := updateBook(t, &librarypb.Book{
			Name:            original.Name,
			PublicationYear: 1984,
		}, []string{"publication_year"})

		require.Equal(t, int32(1984), updated.PublicationYear)
		require.Equal(t, original.Title, updated.Title)

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("EnumField", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "SingleField Enum Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:  original.Name,
			Genre: librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY,
		}, []string{"genre"})

		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, updated.Genre)
		require.Equal(t, original.DisplayName, updated.DisplayName)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ResourceReferenceField", func(t *testing.T) {
		t.Parallel()
		author1 := createTestAuthor(t, organizationParent, "SingleField Ref Author1")
		author2 := createTestAuthor(t, organizationParent, "SingleField Ref Author2")
		shelf := createTestShelf(t, organizationParent, "SingleField Ref Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author1.Name, "SingleField Ref Book")

		updated := updateBook(t, &librarypb.Book{
			Name:   original.Name,
			Author: author2.Name,
		}, []string{"author"})

		require.Equal(t, author2.Name, updated.Author)
		require.Equal(t, original.Title, updated.Title)

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_MultipleFields(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("TwoFields", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "MultiField Two Original")

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			DisplayName: "MultiField Two Updated",
			Biography:   "Updated biography for two fields.",
		}, []string{"display_name", "biography"})

		require.Equal(t, "MultiField Two Updated", updated.DisplayName)
		require.Equal(t, "Updated biography for two fields.", updated.Biography)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("AllSimpleAuthorFields", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "MultiField All Original")

		updated := updateAuthor(t, &librarypb.Author{
			Name:         original.Name,
			DisplayName:  "MultiField All Updated",
			Biography:    "All fields bio.",
			EmailAddress: "all-fields-updated@example.com",
			PhoneNumber:  "+33612345678",
		}, []string{"display_name", "biography", "email_address", "phone_number"})

		require.Equal(t, "MultiField All Updated", updated.DisplayName)
		require.Equal(t, "All fields bio.", updated.Biography)
		require.Equal(t, "all-fields-updated@example.com", updated.EmailAddress)
		require.Equal(t, "+33612345678", updated.PhoneNumber)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("MixedTypesOnBook", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "MultiField Mixed Author")
		shelf := createTestShelf(t, organizationParent, "MultiField Mixed Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "MultiField Mixed Book")

		updated := updateBook(t, &librarypb.Book{
			Name:            original.Name,
			Title:           "Mixed Updated Title",
			Isbn:            "978-1111111111",
			PublicationYear: 2025,
			PageCount:       999,
		}, []string{"title", "isbn", "publication_year", "page_count"})

		require.Equal(t, "Mixed Updated Title", updated.Title)
		require.Equal(t, "978-1111111111", updated.Isbn)
		require.Equal(t, int32(2025), updated.PublicationYear)
		require.Equal(t, int32(999), updated.PageCount)
		require.Equal(t, author.Name, updated.Author)

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_NestedMessage(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("FullReplacement", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Nested Full Original"
		author.Metadata = &librarypb.AuthorMetadata{
			Country:        "US",
			EmailAddresses: []string{"old@test.com"},
			PhoneNumbers:   []string{"+14155551111"},
		}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		newMetadata := &librarypb.AuthorMetadata{
			Country:        "FR",
			EmailAddresses: []string{"new1@test.com", "new2@test.com"},
			PhoneNumbers:   []string{"+33612345678"},
		}
		updated := updateAuthor(t, &librarypb.Author{
			Name:     original.Name,
			Metadata: newMetadata,
		}, []string{"metadata"})

		require.Equal(t, "FR", updated.Metadata.Country)
		require.Equal(t, []string{"new1@test.com", "new2@test.com"}, updated.Metadata.EmailAddresses)
		require.Equal(t, []string{"+33612345678"}, updated.Metadata.PhoneNumbers)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("SingleSubField_PreservesSiblings", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Nested Sub Original"
		author.Metadata = &librarypb.AuthorMetadata{
			Country:        "UK",
			EmailAddresses: []string{"keep@test.com"},
			PhoneNumbers:   []string{"+442079460958"},
		}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name: original.Name,
			Metadata: &librarypb.AuthorMetadata{
				Country: "DE",
			},
		}, []string{"metadata.country"})

		require.Equal(t, "DE", updated.Metadata.Country)
		require.Equal(t, original.Metadata.EmailAddresses, updated.Metadata.EmailAddresses)
		require.Equal(t, original.Metadata.PhoneNumbers, updated.Metadata.PhoneNumbers)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("MultipleSubFields_PreservesSiblings", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Nested MultiSub Original"
		author.Metadata = &librarypb.AuthorMetadata{
			Country:        "JP",
			EmailAddresses: []string{"japan@test.com"},
			PhoneNumbers:   []string{"+81312345678"},
		}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name: original.Name,
			Metadata: &librarypb.AuthorMetadata{
				Country:        "KR",
				EmailAddresses: []string{"korea@test.com"},
			},
		}, []string{"metadata.country", "metadata.email_addresses"})

		require.Equal(t, "KR", updated.Metadata.Country)
		require.Equal(t, []string{"korea@test.com"}, updated.Metadata.EmailAddresses)
		require.Equal(t, original.Metadata.PhoneNumbers, updated.Metadata.PhoneNumbers)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("BookMetadataPartial", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Nested Book Author")
		shelf := createTestShelf(t, organizationParent, "Nested Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "Nested Book Meta")

		updated := updateBook(t, &librarypb.Book{
			Name: original.Name,
			Metadata: &librarypb.BookMetadata{
				Summary: "Only summary changed.",
			},
		}, []string{"metadata.summary"})

		require.Equal(t, "Only summary changed.", updated.Metadata.Summary)
		require.Equal(t, original.Metadata.Language, updated.Metadata.Language)

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ShelfMetadataPartial_WithColumnRename", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "Nested Shelf Meta", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:     original.Name,
			Metadata: &librarypb.ShelfMetadata{Capacity: 500},
		}, []string{"metadata.capacity"})

		require.Equal(t, int32(500), updated.Metadata.Capacity)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_Labels(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("SetOnEmpty", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Labels SetOnEmpty Author")
		require.Empty(t, original.Labels)

		updated := updateAuthor(t, &librarypb.Author{
			Name:   original.Name,
			Labels: map[string]string{"env": "prod", "tier": "gold"},
		}, []string{"labels"})

		require.Equal(t, "prod", updated.Labels["env"])
		require.Equal(t, "gold", updated.Labels["tier"])

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ReplaceExisting", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Labels Replace Author"
		author.Labels = map[string]string{"old-key": "old-value", "keep": "nope"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name:   original.Name,
			Labels: map[string]string{"new-key": "new-value"},
		}, []string{"labels"})

		require.Equal(t, "new-value", updated.Labels["new-key"])
		_, hasOld := updated.Labels["old-key"]
		require.False(t, hasOld)
		_, hasKeep := updated.Labels["keep"]
		require.False(t, hasKeep)
		require.Len(t, updated.Labels, 1)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ClearLabels", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Labels Clear Author"
		author.Labels = map[string]string{"remove": "me"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.NotEmpty(t, original.Labels)

		updated := updateAuthor(t, &librarypb.Author{
			Name:   original.Name,
			Labels: map[string]string{},
		}, []string{"labels"})

		require.Empty(t, updated.Labels)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("PreservedWhenUpdatingOtherFields", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Labels Preserved Author"
		author.Labels = map[string]string{"keep": "this"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Changed bio only.",
		}, []string{"biography"})

		require.Equal(t, "this", updated.Labels["keep"])
		require.Equal(t, "Changed bio only.", updated.Biography)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_FieldPreservation(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("SingleFieldUpdatePreservesAll", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Preserve All Original"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			DisplayName: "Preserve All Changed",
		}, []string{"display_name"})

		expected := proto.CloneOf(original)
		expected.DisplayName = "Preserve All Changed"
		grpcrequire.Equal(t, expected, updated,
			protocmp.IgnoreFields((*librarypb.Author)(nil), "update_time", "etag"))

		require.Equal(t, original.Biography, updated.Biography)
		require.Equal(t, original.EmailAddress, updated.EmailAddress)
		require.Equal(t, original.PhoneNumber, updated.PhoneNumber)
		require.Equal(t, original.EmailAddresses, updated.EmailAddresses)
		require.Equal(t, original.PhoneNumbers, updated.PhoneNumbers)
		require.Equal(t, original.Metadata.Country, updated.Metadata.Country)
		require.Equal(t, original.CreateTime.AsTime(), updated.CreateTime.AsTime())
	})

	t.Run("NestedUpdatePreservesTopLevel", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Nested Preserve Top"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name:     original.Name,
			Metadata: &librarypb.AuthorMetadata{Country: "ZZ"},
		}, []string{"metadata.country"})

		require.Equal(t, original.DisplayName, updated.DisplayName)
		require.Equal(t, original.Biography, updated.Biography)
		require.Equal(t, original.EmailAddress, updated.EmailAddress)
		require.Equal(t, original.PhoneNumber, updated.PhoneNumber)
		require.Equal(t, "ZZ", updated.Metadata.Country)
	})

	t.Run("TopLevelUpdatePreservesNested", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Top Preserve Nested"
		author.Metadata = &librarypb.AuthorMetadata{
			Country:        "Original Country",
			EmailAddresses: []string{"nested@example.com"},
		}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			DisplayName: "Top Changed",
		}, []string{"display_name"})

		require.Equal(t, "Top Changed", updated.DisplayName)
		require.Equal(t, original.Metadata.Country, updated.Metadata.Country)
		require.Equal(t, original.Metadata.EmailAddresses, updated.Metadata.EmailAddresses)
	})

	t.Run("BookUpdatePreservesUnmaskedFields", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Preserve Book Author")
		shelf := createTestShelf(t, organizationParent, "Preserve Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "Preserve Book Title")

		updated := updateBook(t, &librarypb.Book{
			Name:  original.Name,
			Title: "Only Title Changed",
		}, []string{"title"})

		expected := proto.CloneOf(original)
		expected.Title = "Only Title Changed"
		grpcrequire.Equal(t, expected, updated,
			protocmp.IgnoreFields((*librarypb.Book)(nil), "update_time", "etag"))
	})
}

func TestUpdate_TimestampAndEtag(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("UpdateTimeAdvances", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Timestamp Advance Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Timestamp test.",
		}, []string{"biography"})

		require.True(t,
			updated.UpdateTime.AsTime().After(original.UpdateTime.AsTime()) ||
				updated.UpdateTime.AsTime().Equal(original.UpdateTime.AsTime()))
	})

	t.Run("CreateTimeUnchanged", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "CreateTime Unchanged Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Create time test.",
		}, []string{"biography"})

		require.Equal(t, original.CreateTime.AsTime(), updated.CreateTime.AsTime())
	})

	t.Run("UpdateTimeAccurate", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "UpdateTime Accurate Author")

		before := time.Now().UTC()
		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Accurate timestamp test.",
		}, []string{"biography"})
		after := time.Now().UTC()

		updateTime := updated.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before), "update_time %v should be >= before %v", updateTime, before)
		require.True(t, !updateTime.After(after), "update_time %v should be <= after %v", updateTime, after)

		got := getAuthor(t, original.Name)
		require.Equal(t, updated.UpdateTime, got.UpdateTime)
	})

	t.Run("EtagChanges", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Etag Change Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Etag test.",
		}, []string{"biography"})

		require.NotEmpty(t, updated.Etag)
		require.NotEqual(t, original.Etag, updated.Etag)
	})

	t.Run("ConsecutiveUpdatesChangeEtag", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Consecutive Etag Author")

		first := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "First update.",
		}, []string{"biography"})

		second := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Second update.",
		}, []string{"biography"})

		require.NotEqual(t, original.Etag, first.Etag)
		require.NotEqual(t, first.Etag, second.Etag)
		require.NotEqual(t, original.Etag, second.Etag)
	})

	t.Run("EtagMatchesGetAfterUpdate", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Etag Get Match Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Etag get match test.",
		}, []string{"biography"})

		got := getAuthor(t, original.Name)
		require.Equal(t, updated.Etag, got.Etag)
	})

	t.Run("DeleteTimeStaysNilAfterUpdate", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "DeleteTime Nil Author")
		require.Nil(t, original.DeleteTime)

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "Delete time nil test.",
		}, []string{"biography"})

		require.Nil(t, updated.DeleteTime)
	})
}

func TestUpdate_ZeroValues(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("ClearStringField", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "ZeroVal String Author")
		require.NotEmpty(t, original.Biography)

		updated := updateAuthor(t, &librarypb.Author{
			Name:      original.Name,
			Biography: "",
		}, []string{"biography"})

		require.Empty(t, updated.Biography)

		got := getAuthor(t, original.Name)
		require.Empty(t, got.Biography)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("SetIntegerToZero", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "ZeroVal Int Author")
		shelf := createTestShelf(t, organizationParent, "ZeroVal Int Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "ZeroVal Int Book")
		require.NotEqual(t, int32(0), original.PublicationYear)

		updated := updateBook(t, &librarypb.Book{
			Name:            original.Name,
			PublicationYear: 0,
		}, []string{"publication_year"})

		require.Equal(t, int32(0), updated.PublicationYear)

		got := getBook(t, original.Name)
		require.Equal(t, int32(0), got.PublicationYear)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("SetPageCountToZero", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "ZeroVal Page Author")
		shelf := createTestShelf(t, organizationParent, "ZeroVal Page Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "ZeroVal Page Book")
		require.NotEqual(t, int32(0), original.PageCount)

		updated := updateBook(t, &librarypb.Book{
			Name:      original.Name,
			PageCount: 0,
		}, []string{"page_count"})

		require.Equal(t, int32(0), updated.PageCount)

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ClearNestedMessageField", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "ZeroVal Nested Author"
		author.Metadata = &librarypb.AuthorMetadata{
			Country:        "HasCountry",
			EmailAddresses: []string{"keep@test.com"},
		}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name: original.Name,
			Metadata: &librarypb.AuthorMetadata{
				Country: "",
			},
		}, []string{"metadata.country"})

		require.Empty(t, updated.Metadata.Country)
		require.Equal(t, original.Metadata.EmailAddresses, updated.Metadata.EmailAddresses)

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_Canonicalization(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("EmailLowercased", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Canon Email Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:         original.Name,
			EmailAddress: "UPPER@EXAMPLE.COM",
		}, []string{"email_address"})

		require.Equal(t, "upper@example.com", updated.EmailAddress)

		got := getAuthor(t, original.Name)
		require.Equal(t, "upper@example.com", got.EmailAddress)
	})

	t.Run("PhoneNumberE164_US", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Canon Phone US Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			PhoneNumber: "(415) 555-0199",
		}, []string{"phone_number"})

		require.Equal(t, "+14155550199", updated.PhoneNumber)

		got := getAuthor(t, original.Name)
		require.Equal(t, "+14155550199", got.PhoneNumber)
	})

	t.Run("PhoneNumberE164_French", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Canon Phone FR Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:        original.Name,
			PhoneNumber: "+33 6 12 34 56 78",
		}, []string{"phone_number"})

		require.Equal(t, "+33612345678", updated.PhoneNumber)
	})

	t.Run("NestedMetadataEmailsCanonicalized", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Canon Nested Author"
		author.Metadata = &librarypb.AuthorMetadata{Country: "US"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name: original.Name,
			Metadata: &librarypb.AuthorMetadata{
				EmailAddresses: []string{"ALICE@EXAMPLE.COM", "BOB@EXAMPLE.COM"},
			},
		}, []string{"metadata.email_addresses"})

		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, updated.Metadata.EmailAddresses)

		got := getAuthor(t, original.Name)
		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, got.Metadata.EmailAddresses)
	})

	t.Run("NestedMetadataPhonesCanonicalized", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = "Canon Nested Phone Author"
		author.Metadata = &librarypb.AuthorMetadata{Country: "UK"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		original, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updated := updateAuthor(t, &librarypb.Author{
			Name: original.Name,
			Metadata: &librarypb.AuthorMetadata{
				PhoneNumbers: []string{"+44 20 7946 0958"},
			},
		}, []string{"metadata.phone_numbers"})

		require.Equal(t, []string{"+442079460958"}, updated.Metadata.PhoneNumbers)

		got := getAuthor(t, original.Name)
		require.Equal(t, []string{"+442079460958"}, got.Metadata.PhoneNumbers)
	})
}

func TestUpdate_ColumnNameReplacement(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("ExternalId", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "ColName ExtId Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:       original.Name,
			ExternalId: "ext-updated-123",
		}, []string{"external_id"})

		require.Equal(t, "ext-updated-123", updated.ExternalId)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("CorrelationId2", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "ColName CorrId Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:            original.Name,
			CorrelationId_2: "corr-updated-456",
		}, []string{"correlation_id_2"})

		require.Equal(t, "corr-updated-456", updated.CorrelationId_2)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("BothRenamedColumns", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "ColName Both Shelf", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:            original.Name,
			ExternalId:      "ext-both-789",
			CorrelationId_2: "corr-both-101",
		}, []string{"external_id", "correlation_id_2"})

		require.Equal(t, "ext-both-789", updated.ExternalId)
		require.Equal(t, "corr-both-101", updated.CorrelationId_2)
		require.Equal(t, original.DisplayName, updated.DisplayName)
		require.Equal(t, original.Genre, updated.Genre)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("RenamedColumnWithStandardColumn", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "ColName Mixed Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:        original.Name,
			DisplayName: "ColName Mixed Updated",
			ExternalId:  "ext-mixed-222",
		}, []string{"display_name", "external_id"})

		require.Equal(t, "ColName Mixed Updated", updated.DisplayName)
		require.Equal(t, "ext-mixed-222", updated.ExternalId)

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}

func TestUpdate_Unauthorized(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	original := createTestAuthor(t, organizationParent, "Unauthorized Fields Author")

	tests := []struct {
		name  string
		paths []string
	}{
		{"Name", []string{"name"}},
		{"CreateTime", []string{"create_time"}},
		{"DeleteTime", []string{"delete_time"}},
		{"EmailAddresses_NotInPaths", []string{"email_addresses"}},
		{"PhoneNumbers_NotInPaths", []string{"phone_numbers"}},
		{"MixAuthorizedAndUnauthorized", []string{"display_name", "name"}},
		{"MixAuthorizedAndNotInPaths", []string{"display_name", "email_addresses"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
				Author: &librarypb.Author{
					Name: original.Name,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.paths},
			}
			_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}

	t.Run("BookLabelsNotInPaths", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Unauth Book Author")
		shelf := createTestShelf(t, organizationParent, "Unauth Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "Unauth Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:   book.Name,
				Labels: map[string]string{"key": "val"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("ShelfLabelsNotInPaths", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Unauth Shelf Labels", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:   shelf.Name,
				Labels: map[string]string{"key": "val"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("ShelfMetadataFullNotInPaths", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Unauth Shelf Meta", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:     shelf.Name,
				Metadata: &librarypb.ShelfMetadata{Capacity: 999},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestUpdate_InvalidMask(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("EmptyMask", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Invalid Empty Mask Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author:     &librarypb.Author{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NonexistentField", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Invalid Nonexistent Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author:     &librarypb.Author{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"nonexistent_field"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingName", func(t *testing.T) {
		t.Parallel()
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				DisplayName: "No Name Set",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NilMask", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "Invalid Nil Mask Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{Name: original.Name},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestUpdate_NotFound(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:        organizationParent + "/authors/nonexistent-update",
				DisplayName: "Ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "NotFound Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  shelf.Name + "/books/nonexistent-update",
				Title: "Ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Shelf", func(t *testing.T) {
		t.Parallel()
		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        organizationParent + "/shelves/nonexistent-update",
				DisplayName: "Ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestUpdate_IdempotentSameValue(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	original := createTestAuthor(t, organizationParent, "Idempotent Author")

	first := updateAuthor(t, &librarypb.Author{
		Name:        original.Name,
		DisplayName: "Idempotent Value",
	}, []string{"display_name"})

	second := updateAuthor(t, &librarypb.Author{
		Name:        original.Name,
		DisplayName: "Idempotent Value",
	}, []string{"display_name"})

	require.Equal(t, first.DisplayName, second.DisplayName)
	require.NotEqual(t, first.Etag, second.Etag)

	got := getAuthor(t, original.Name)
	grpcrequire.Equal(t, second, got)
}

func TestUpdate_GetMatchesUpdateResponse(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		original := createTestAuthor(t, organizationParent, "GetMatch Author")

		updated := updateAuthor(t, &librarypb.Author{
			Name:         original.Name,
			DisplayName:  "GetMatch Updated",
			Biography:    "GetMatch bio.",
			EmailAddress: "getmatch@example.com",
			Labels:       map[string]string{"a": "b"},
			Metadata:     &librarypb.AuthorMetadata{Country: "NZ"},
		}, []string{"display_name", "biography", "email_address", "labels", "metadata"})

		got := getAuthor(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "GetMatch Book Author")
		shelf := createTestShelf(t, organizationParent, "GetMatch Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		original := createTestBook(t, shelf.Name, author.Name, "GetMatch Book")

		updated := updateBook(t, &librarypb.Book{
			Name:            original.Name,
			Title:           "GetMatch Book Updated",
			Isbn:            "978-9999999999",
			PublicationYear: 2026,
			PageCount:       42,
		}, []string{"title", "isbn", "publication_year", "page_count"})

		got := getBook(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("Shelf", func(t *testing.T) {
		t.Parallel()
		original := createTestShelf(t, organizationParent, "GetMatch Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updated := updateShelf(t, &librarypb.Shelf{
			Name:            original.Name,
			DisplayName:     "GetMatch Shelf Updated",
			Genre:           librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION,
			ExternalId:      "getmatch-ext",
			CorrelationId_2: "getmatch-corr",
		}, []string{"display_name", "genre", "external_id", "correlation_id_2"})

		got := getShelf(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})
}
