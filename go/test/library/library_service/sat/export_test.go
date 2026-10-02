package sat

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// exportBooks runs an export of the books under parent to completion.
func exportBooks(t *testing.T, request *libraryservicepb.ExportBooksRequest) *longrunningpb.Operation {
	t.Helper()
	request.RequestId = uuid.MustNewV7().String()
	operation, err := libraryServiceClient.ExportBooks(ctx, request)
	require.NoError(t, err)
	done := waitOperation(t, operation.GetName(), operationWaitTimeout)
	require.True(t, done.GetDone())
	require.Nil(t, done.GetError())
	return done
}

func inlineExportBooksRequest(parent, filter string) *libraryservicepb.ExportBooksRequest {
	return &libraryservicepb.ExportBooksRequest{
		Parent:      parent,
		Filter:      filter,
		Destination: &libraryservicepb.ExportBooksRequest_InlineDestination_{InlineDestination: &libraryservicepb.ExportBooksRequest_InlineDestination{}},
	}
}

func exportMetadata(t *testing.T, operation *longrunningpb.Operation) *aippb.ExportMetadata {
	t.Helper()
	require.NotNil(t, operation.GetMetadata(), "operation %s has no metadata", operation.GetName())
	return unpackAny[*aippb.ExportMetadata](t, operation.GetMetadata())
}

// exportedBooks returns the response of a finished inline export, its books by
// name: the export pages in primary key order, which the database collation decides.
func exportedBooks(t *testing.T, done *longrunningpb.Operation) *libraryservicepb.ExportBooksResponse {
	t.Helper()
	response := unpackAny[*libraryservicepb.ExportBooksResponse](t, done.GetResponse())
	slices.SortFunc(response.Books, func(a, b *librarypb.Book) int { return strings.Compare(a.GetName(), b.GetName()) })
	return response
}

// listedBooks returns the books under parent as ListBooks has them, by name.
func listedBooks(t *testing.T, parent string) []*librarypb.Book {
	t.Helper()
	listBooksRequest := &libraryservicepb.ListBooksRequest{Parent: parent, PageSize: 1000}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	books := listBooksResponse.GetBooks()
	slices.SortFunc(books, func(a, b *librarypb.Book) int { return strings.Compare(a.GetName(), b.GetName()) })
	return books
}

func TestExportBooks_Inline(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	for _, title := range titles(3) {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}

	done := exportBooks(t, inlineExportBooksRequest(fixture.shelf.GetName(), ""))
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 3}, exportMetadata(t, done))
	expected := &libraryservicepb.ExportBooksResponse{Books: listedBooks(t, fixture.shelf.GetName())}
	require.Len(t, expected.GetBooks(), 3)
	grpcrequire.Equal(t, expected, exportedBooks(t, done))
}

// What an inline export returns, an inline import takes back (AIP-153).
func TestExportBooks_RoundTrip(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	for _, title := range titles(2) {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}
	exported := exportedBooks(t, exportBooks(t, inlineExportBooksRequest(fixture.shelf.GetName(), ""))).GetBooks()

	target := newImportFixture(t)
	shelf, err := librarypb.ParseShelfRn(target.shelf.GetName())
	require.NoError(t, err)
	for _, book := range exported {
		bookRn, err := librarypb.ParseBookRn(book.GetName())
		require.NoError(t, err)
		book.Name = shelf.BookRn(bookRn.Book).String()
	}
	imported := waitOperation(t, target.importBooks(t, target.inlineRequest(exported...)).GetName(), operationWaitTimeout)
	require.Nil(t, imported.GetError())
	grpcrequire.Equal(t, &aippb.ImportMetadata{SuccessCount: 2, TotalCount: 2}, importMetadata(t, imported))
}

func TestExportBooks_Filter(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(3)
	for _, title := range titles {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}

	done := exportBooks(t, inlineExportBooksRequest(fixture.shelf.GetName(), fmt.Sprintf("title = %q", titles[1])))
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 1}, exportMetadata(t, done))
	books := exportedBooks(t, done).GetBooks()
	require.Len(t, books, 1)
	require.Equal(t, titles[1], books[0].GetTitle())
}

func TestExportBooks_WildcardParent(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	otherShelf := createTestShelf(t, fixture.organization, "Other Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	titles := titles(2)
	createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[0])
	createTestBook(t, otherShelf.GetName(), fixture.author.GetName(), titles[1])

	done := exportBooks(t, inlineExportBooksRequest(fixture.organization+"/shelves/-", ""))
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 2}, exportMetadata(t, done))
	expected := &libraryservicepb.ExportBooksResponse{Books: listedBooks(t, fixture.organization+"/shelves/-")}
	require.Len(t, expected.GetBooks(), 2)
	grpcrequire.Equal(t, expected, exportedBooks(t, done))
}

// More books than a page: the keyset carries the export over the page boundary
// without skipping or repeating a book.
func TestExportBooks_Paging(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(1001)
	books := make([]*librarypb.Book, len(titles))
	for i, title := range titles {
		books[i] = inlineBook(fixture.author.GetName(), title)
	}
	imported := waitOperation(t, fixture.importBooks(t, fixture.inlineRequest(books...)).GetName(), operationWaitTimeout)
	require.Nil(t, imported.GetError())

	done := exportBooks(t, inlineExportBooksRequest(fixture.shelf.GetName(), ""))
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: int32(len(titles))}, exportMetadata(t, done))
	exported := exportedBooks(t, done).GetBooks()
	exportedTitles := make([]string, len(exported))
	for i, book := range exported {
		exportedTitles[i] = book.GetTitle()
	}
	require.ElementsMatch(t, titles, exportedTitles)
}

func TestExportBooks_Csv(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(2)
	kept := createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[0])
	rejected := createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[1])

	done := exportBooks(t, &libraryservicepb.ExportBooksRequest{
		Parent:      fixture.shelf.GetName(),
		Destination: &libraryservicepb.ExportBooksRequest_CsvDestination{CsvDestination: &libraryservicepb.CsvDestination{RejectTitle: titles[1]}},
	})
	metadata := exportMetadata(t, done)
	require.Len(t, metadata.GetErrors(), 1)
	require.Equal(t, int32(codes.InvalidArgument), metadata.GetErrors()[0].GetCode())
	require.Equal(t, fmt.Sprintf("book %q is rejected", rejected.GetName()), metadata.GetErrors()[0].GetMessage())
	metadata.Errors = nil
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 1, FailureCount: 1}, metadata)
	expected := &libraryservicepb.ExportBooksResponse{Csv: fmt.Sprintf("name,title\n%s,%s\n", kept.GetName(), titles[0])}
	grpcrequire.Equal(t, expected, unpackAny[*libraryservicepb.ExportBooksResponse](t, done.GetResponse()))
}

func TestExportShelves(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	for _, title := range titles(2) {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}
	deleted := createTestShelf(t, fixture.organization, "Deleted Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: deleted.GetName()}
	_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
	require.NoError(t, err)

	exportShelves := func(showDeleted bool) *libraryservicepb.ExportShelvesResponse {
		t.Helper()
		exportShelvesRequest := &libraryservicepb.ExportShelvesRequest{
			Parent:      fixture.organization,
			ShowDeleted: showDeleted,
			RequestId:   uuid.MustNewV7().String(),
			Destination: &libraryservicepb.ExportShelvesRequest_InlineDestination_{InlineDestination: &libraryservicepb.ExportShelvesRequest_InlineDestination{}},
		}
		operation, err := libraryServiceClient.ExportShelves(ctx, exportShelvesRequest)
		require.NoError(t, err)
		done := waitOperation(t, operation.GetName(), operationWaitTimeout)
		require.Nil(t, done.GetError())
		response := unpackAny[*libraryservicepb.ExportShelvesResponse](t, done.GetResponse())
		grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: int32(len(response.GetShelves()))}, exportMetadata(t, done))
		slices.SortFunc(response.Shelves, func(a, b *libraryservicepb.ExportedShelf) int {
			return strings.Compare(a.GetShelf().GetName(), b.GetShelf().GetName())
		})
		return response
	}

	// expected is the export of the organization's shelves as ListShelves has them, each with its books.
	expected := func(showDeleted bool) *libraryservicepb.ExportShelvesResponse {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{Parent: fixture.organization, ShowDeleted: showDeleted, PageSize: 1000}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		response := &libraryservicepb.ExportShelvesResponse{}
		for _, shelf := range listShelvesResponse.GetShelves() {
			response.Shelves = append(response.Shelves, &libraryservicepb.ExportedShelf{Shelf: shelf, Books: listedBooks(t, shelf.GetName())})
		}
		slices.SortFunc(response.Shelves, func(a, b *libraryservicepb.ExportedShelf) int {
			return strings.Compare(a.GetShelf().GetName(), b.GetShelf().GetName())
		})
		return response
	}

	live := expected(false)
	require.Len(t, live.GetShelves(), 1)
	require.Len(t, live.GetShelves()[0].GetBooks(), 2)
	grpcrequire.Equal(t, live, exportShelves(false))

	all := expected(true)
	require.Len(t, all.GetShelves(), 2)
	grpcrequire.Equal(t, all, exportShelves(true))
}
