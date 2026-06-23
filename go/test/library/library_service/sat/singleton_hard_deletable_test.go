package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func getBookReview(t *testing.T, name string) *librarypb.BookReview {
	t.Helper()
	getBookReviewRequest := &libraryservicepb.GetBookReviewRequest{Name: name}
	bookReview, err := libraryServiceClient.GetBookReview(ctx, getBookReviewRequest)
	require.NoError(t, err)
	return bookReview
}

func TestBookReview_AutoCreatedWithBook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Auto Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Auto Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview Auto Book")

	reviewName := book.Name + "/review"
	review := getBookReview(t, reviewName)
	require.Equal(t, reviewName, review.Name)
	require.NotNil(t, review.CreateTime)
	require.NotNil(t, review.UpdateTime)
	require.Equal(t, book.CreateTime.AsTime(), review.CreateTime.AsTime())
	require.NotEmpty(t, review.Etag)
	require.Equal(t, int32(0), review.Rating)
	require.Empty(t, review.Comment)
}

func TestBookReview_AutoCreatedWithBook_IdempotentCreation(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Idempotent Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Idempotent Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelf.Name,
		Book: &librarypb.Book{
			Title:    "Idempotent Book",
			Author:   author.Name,
			Duration: durationpb.New(100 * time.Second),
			Metadata: &librarypb.BookMetadata{},
		},
		RequestId: "d3b07384-d9a3-4f1b-8b0d-000000000001",
	}

	book1, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)

	book2, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	require.Equal(t, book1.Name, book2.Name)

	review := getBookReview(t, book1.Name+"/review")
	require.Equal(t, book1.Name+"/review", review.Name)
	require.Equal(t, book1.CreateTime.AsTime(), review.CreateTime.AsTime())
}

func TestBookReview_JoinFieldsPopulated(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Join Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Join Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelf.Name,
		Book: &librarypb.Book{
			Title:           "Join Test Title",
			Author:          author.Name,
			PublicationYear: 1984,
			Duration:        durationpb.New(100 * time.Second),
			Metadata:        &librarypb.BookMetadata{},
		},
	}
	book, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)

	review := getBookReview(t, book.Name+"/review")
	require.Equal(t, "Join Test Title", review.BookTitle)
	require.Equal(t, int32(1984), review.BookPublicationYear)
}

func TestBookReview_JoinFieldsReflectParentUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview JoinUpd Author")
	shelf := createTestShelf(t, organizationParent, "BookReview JoinUpd Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBookWithYear(t, shelf.Name, author.Name, "Original Title", 2000)

	review := getBookReview(t, book.Name+"/review")
	require.Equal(t, "Original Title", review.BookTitle)
	require.Equal(t, int32(2000), review.BookPublicationYear)

	updateBookRequest := &libraryservicepb.UpdateBookRequest{
		Book: &librarypb.Book{
			Name:            book.Name,
			Title:           "Updated Title",
			PublicationYear: 2025,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "publication_year"}},
	}
	_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
	require.NoError(t, err)

	review = getBookReview(t, book.Name+"/review")
	require.Equal(t, "Updated Title", review.BookTitle)
	require.Equal(t, int32(2025), review.BookPublicationYear)
}

func TestBookReview_Update(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Update Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("UpdateRatingAndComment", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 1")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Rating:  5,
				Comment: "Excellent book!",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating", "comment"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.Equal(t, int32(5), updatedReview.Rating)
		require.Equal(t, "Excellent book!", updatedReview.Comment)

		gotReview := getBookReview(t, reviewName)
		grpcrequire.Equal(t, updatedReview, gotReview)
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 2")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:   reviewName,
				Labels: map[string]string{"verified": "true"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.Equal(t, "true", updatedReview.Labels["verified"])
	})

	t.Run("UpdateMetadata", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 3")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
				Metadata: &librarypb.BookReviewMetadata{
					ReviewerName: "Alice",
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.Equal(t, "Alice", updatedReview.Metadata.ReviewerName)
	})

	t.Run("EtagChanges", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 4")
		reviewName := book.Name + "/review"
		original := getBookReview(t, reviewName)
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Comment: "Etag test comment.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.NotEqual(t, original.Etag, updatedReview.Etag)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 5")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyUpdateMask", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 6")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Comment: "Should fail",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("PartialUpdatePreservesOtherFields", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 7")
		reviewName := book.Name + "/review"

		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Rating:  4,
				Comment: "Great read",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating", "comment"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)

		updateBookReviewRequest = &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:   reviewName,
				Rating: 5,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.Equal(t, int32(5), updatedReview.Rating)
		require.Equal(t, "Great read", updatedReview.Comment)
	})

	t.Run("EtagMismatch_WithExplicitEtag", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 8")
		reviewName := book.Name + "/review"
		original := getBookReview(t, reviewName)

		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Comment: "First update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)

		updateBookReviewRequest = &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Etag:    original.Etag,
				Comment: "Stale update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
		}
		_, err = libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("EtagMismatch_WithoutExplicitEtag_Succeeds", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 9")
		reviewName := book.Name + "/review"

		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Comment: "No etag update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.Equal(t, "No etag update", updatedReview.Comment)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 10")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_UpdateTime", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 11")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"update_time"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_Etag", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 12")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"etag"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_JoinField", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 13")
		reviewName := book.Name + "/review"
		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name: reviewName,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"book_title"}},
		}
		_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UpdateTimeAdvances", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book 14")
		reviewName := book.Name + "/review"
		original := getBookReview(t, reviewName)

		updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
			BookReview: &librarypb.BookReview{
				Name:    reviewName,
				Comment: "Time check",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
		}
		updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
		require.NoError(t, err)
		require.True(t, updatedReview.UpdateTime.AsTime().After(original.UpdateTime.AsTime()) ||
			updatedReview.UpdateTime.AsTime().Equal(original.UpdateTime.AsTime()))
		require.Equal(t, original.CreateTime.AsTime(), updatedReview.CreateTime.AsTime())
	})
}

func TestBookReview_JoinFieldsOnUpdate(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview JoinOnUpd Author")
	shelf := createTestShelf(t, organizationParent, "BookReview JoinOnUpd Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "JoinOnUpd Book")
	reviewName := book.Name + "/review"

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:   reviewName,
			Rating: 4,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
	}
	updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)
	require.Equal(t, "JoinOnUpd Book", updatedReview.BookTitle)
	require.Equal(t, int32(4), updatedReview.Rating)
}

func TestBookReview_GetNotFound(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "BookReview NF Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	getBookReviewRequest := &libraryservicepb.GetBookReviewRequest{
		Name: shelf.Name + "/books/nonexistent-book/review",
	}
	_, err := libraryServiceClient.GetBookReview(ctx, getBookReviewRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestBookReview_MultipleBooks_IndependentReviews(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Multi Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Multi Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	bookA := createTestBookWithYear(t, shelf.Name, author.Name, "Book A", 1990)
	bookB := createTestBookWithYear(t, shelf.Name, author.Name, "Book B", 2020)

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:   bookA.Name + "/review",
			Rating: 3,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
	}
	_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)

	reviewA := getBookReview(t, bookA.Name+"/review")
	reviewB := getBookReview(t, bookB.Name+"/review")

	require.Equal(t, int32(3), reviewA.Rating)
	require.Equal(t, int32(0), reviewB.Rating)
	require.Equal(t, "Book A", reviewA.BookTitle)
	require.Equal(t, int32(1990), reviewA.BookPublicationYear)
	require.Equal(t, "Book B", reviewB.BookTitle)
	require.Equal(t, int32(2020), reviewB.BookPublicationYear)
}

func TestBookReview_DeletedWithParent(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Del Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Del Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview Del Book")
	reviewName := book.Name + "/review"

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:   reviewName,
			Rating: 5,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
	}
	_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)

	deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
	_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
	require.NoError(t, err)

	getBookReviewRequest := &libraryservicepb.GetBookReviewRequest{Name: reviewName}
	_, err = libraryServiceClient.GetBookReview(ctx, getBookReviewRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestBookReview_DeletedWithParent_ThenUpdate_Fails(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview DelUpd Author")
	shelf := createTestShelf(t, organizationParent, "BookReview DelUpd Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview DelUpd Book")
	reviewName := book.Name + "/review"

	deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
	_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
	require.NoError(t, err)

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:    reviewName,
			Comment: "Should fail",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment"}},
	}
	_, err = libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestBookReview_BatchGet(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview BatchGet Author")
	shelf := createTestShelf(t, organizationParent, "BookReview BatchGet Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	parent := shelf.GetName() + "/books/-"

	bookA := createTestBookWithYear(t, shelf.Name, author.Name, "BatchGet Book A", 2001)
	bookB := createTestBookWithYear(t, shelf.Name, author.Name, "BatchGet Book B", 2002)

	reviewNameA := bookA.Name + "/review"
	reviewNameB := bookB.Name + "/review"

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:   reviewNameA,
			Rating: 3,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
	}
	_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetBookReviewsRequest := &libraryservicepb.BatchGetBookReviewsRequest{
			Parent: parent,
			Names:  []string{reviewNameA, reviewNameB},
		}
		batchGetBookReviewsResponse, err := libraryServiceClient.BatchGetBookReviews(ctx, batchGetBookReviewsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBookReviewsResponse.BookReviews, 2)

		reviewNameToBookReview := map[string]*librarypb.BookReview{}
		for _, review := range batchGetBookReviewsResponse.BookReviews {
			reviewNameToBookReview[review.Name] = review
		}
		require.Equal(t, int32(3), reviewNameToBookReview[reviewNameA].Rating)
		require.Equal(t, int32(0), reviewNameToBookReview[reviewNameB].Rating)
		require.Equal(t, "BatchGet Book A", reviewNameToBookReview[reviewNameA].BookTitle)
		require.Equal(t, "BatchGet Book B", reviewNameToBookReview[reviewNameB].BookTitle)
	})

	t.Run("PreservesRequestOrder", func(t *testing.T) {
		t.Parallel()
		batchGetBookReviewsRequest := &libraryservicepb.BatchGetBookReviewsRequest{
			Parent: parent,
			Names:  []string{reviewNameB, reviewNameA},
		}
		batchGetBookReviewsResponse, err := libraryServiceClient.BatchGetBookReviews(ctx, batchGetBookReviewsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBookReviewsResponse.BookReviews, 2)
		require.Equal(t, reviewNameB, batchGetBookReviewsResponse.BookReviews[0].Name)
		require.Equal(t, reviewNameA, batchGetBookReviewsResponse.BookReviews[1].Name)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetBookReviewsRequest := &libraryservicepb.BatchGetBookReviewsRequest{
			Parent: parent,
			Names:  []string{reviewNameA, shelf.Name + "/books/nonexistent-book/review"},
		}
		_, err := libraryServiceClient.BatchGetBookReviews(ctx, batchGetBookReviewsRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestBookReview_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview List Author")
	shelf := createTestShelf(t, organizationParent, "BookReview List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview List Book")

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:   book.Name + "/review",
			Rating: 4,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating"}},
	}
	_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)

	listBookReviewsRequest := &libraryservicepb.ListBookReviewsRequest{
		Parent: book.Name,
	}
	listBookReviewsResponse, err := libraryServiceClient.ListBookReviews(ctx, listBookReviewsRequest)
	require.NoError(t, err)
	require.Len(t, listBookReviewsResponse.BookReviews, 1)
	require.Equal(t, book.Name+"/review", listBookReviewsResponse.BookReviews[0].Name)
	require.Equal(t, int32(4), listBookReviewsResponse.BookReviews[0].Rating)
	require.Equal(t, "BookReview List Book", listBookReviewsResponse.BookReviews[0].BookTitle)
}

func TestBookReview_List_AfterParentDeletion(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview ListDel Author")
	shelf := createTestShelf(t, organizationParent, "BookReview ListDel Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview ListDel Book")

	listBookReviewsRequest := &libraryservicepb.ListBookReviewsRequest{
		Parent: book.Name,
	}
	listBookReviewsResponse, err := libraryServiceClient.ListBookReviews(ctx, listBookReviewsRequest)
	require.NoError(t, err)
	require.Len(t, listBookReviewsResponse.BookReviews, 1)

	deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
	_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
	require.NoError(t, err)

	listBookReviewsResponse, err = libraryServiceClient.ListBookReviews(ctx, listBookReviewsRequest)
	require.NoError(t, err)
	require.Empty(t, listBookReviewsResponse.BookReviews)
}

func TestBookReview_GetWithWildcard(t *testing.T) {
	t.Parallel()
	getBookReviewRequest := &libraryservicepb.GetBookReviewRequest{
		Name: "organizations/-/shelves/-/books/-/review",
	}
	_, err := libraryServiceClient.GetBookReview(ctx, getBookReviewRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestBookReview_Update_ClearableFields(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "BookReview Clear Author")
	shelf := createTestShelf(t, organizationParent, "BookReview Clear Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "BookReview Clear Book")
	reviewName := book.Name + "/review"

	updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:    reviewName,
			Rating:  5,
			Comment: "Will be cleared",
			Labels:  map[string]string{"key": "value"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rating", "comment", "labels"}},
	}
	_, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)

	updateBookReviewRequest = &libraryservicepb.UpdateBookReviewRequest{
		BookReview: &librarypb.BookReview{
			Name:    reviewName,
			Comment: "",
			Labels:  nil,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"comment", "labels"}},
	}
	updatedReview, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
	require.NoError(t, err)
	require.Empty(t, updatedReview.Comment)
	require.Empty(t, updatedReview.Labels)
	require.Equal(t, int32(5), updatedReview.Rating)
}
