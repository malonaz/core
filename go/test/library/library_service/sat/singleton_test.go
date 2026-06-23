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
  book := createTestBook(t, shelf.Name, author.Name, "BookReview Update Book")
  reviewName := book.Name + "/review"

  t.Run("UpdateRatingAndComment", func(t *testing.T) {
    t.Parallel()
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
    updateBookReviewRequest := &libraryservicepb.UpdateBookReviewRequest{
      BookReview: &librarypb.BookReview{
        Name: reviewName,
      },
      UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
    }
    _, err := libraryServiceClient.UpdateBookReview(ctx, updateBookReviewRequest)
    grpcrequire.Error(t, codes.InvalidArgument, err)
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
