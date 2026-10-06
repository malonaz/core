package sat

import (
	"encoding/csv"
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

var csvDestination = &libraryservicepb.ExportBooksRequest_CsvDestination{CsvDestination: &libraryservicepb.CsvDestination{}}

// exportBooks runs a CSV export of the books under parent to completion.
func exportBooks(t *testing.T, parent, filter, rejectTitle string) *longrunningpb.Operation {
	t.Helper()
	return runExportBooks(t, &libraryservicepb.ExportBooksRequest{
		Parent:      parent,
		Filter:      filter,
		Destination: csvDestination,
		RequestId:   uuid.MustNewV7().String(),
		RejectTitle: rejectTitle,
	})
}

// runExportBooks runs an export of books to completion.
func runExportBooks(t *testing.T, request *libraryservicepb.ExportBooksRequest) *longrunningpb.Operation {
	t.Helper()
	operation, err := libraryServiceClient.ExportBooks(ctx, request)
	require.NoError(t, err)
	done := waitOperation(t, operation.GetName(), operationWaitTimeout)
	require.True(t, done.GetDone())
	require.Nil(t, done.GetError())
	return done
}

func exportMetadata(t *testing.T, operation *longrunningpb.Operation) *aippb.ExportMetadata {
	t.Helper()
	require.NotNil(t, operation.GetMetadata(), "operation %s has no metadata", operation.GetName())
	return unpackAny[*aippb.ExportMetadata](t, operation.GetMetadata())
}

// csvRows parses a CSV export, its rows sorted by name: the export pages in
// primary key order, which the database collation decides.
func csvRows(t *testing.T, document string) [][]string {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(document)).ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	body := rows[1:]
	slices.SortFunc(body, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	return append(rows[:1], body...)
}

// exportedBooks returns the rows of a finished books export.
func exportedBooks(t *testing.T, done *longrunningpb.Operation) [][]string {
	t.Helper()
	return csvRows(t, unpackAny[*libraryservicepb.ExportBooksResponse](t, done.GetResponse()).GetCsv())
}

// listedBooks returns the rows the books under parent export to, as ListBooks has them.
func listedBooks(t *testing.T, parent string) [][]string {
	t.Helper()
	listBooksRequest := &libraryservicepb.ListBooksRequest{Parent: parent, PageSize: 1000}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	rows := [][]string{{"name", "title"}}
	for _, book := range listBooksResponse.GetBooks() {
		rows = append(rows, []string{book.GetName(), book.GetTitle()})
	}
	slices.SortFunc(rows[1:], func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	return rows
}

func TestExportBooks(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	for _, title := range titles(3) {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}

	done := exportBooks(t, fixture.shelf.GetName(), "", "")
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 3}, exportMetadata(t, done))
	expected := listedBooks(t, fixture.shelf.GetName())
	require.Len(t, expected, 4)
	require.Equal(t, expected, exportedBooks(t, done))
}

func TestExportBooks_Filter(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(3)
	var books []*librarypb.Book
	for _, title := range titles {
		books = append(books, createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title))
	}

	done := exportBooks(t, fixture.shelf.GetName(), fmt.Sprintf("title = %q", titles[1]), "")
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 1}, exportMetadata(t, done))
	require.Equal(t, [][]string{{"name", "title"}, {books[1].GetName(), titles[1]}}, exportedBooks(t, done))
}

func TestExportBooks_WildcardParent(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	otherShelf := createTestShelf(t, fixture.organization, "Other Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	titles := titles(2)
	createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[0])
	createTestBook(t, otherShelf.GetName(), fixture.author.GetName(), titles[1])

	done := exportBooks(t, fixture.organization+"/shelves/-", "", "")
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 2}, exportMetadata(t, done))
	expected := listedBooks(t, fixture.organization+"/shelves/-")
	require.Len(t, expected, 3)
	require.Equal(t, expected, exportedBooks(t, done))
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

	done := exportBooks(t, fixture.shelf.GetName(), "", "")
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: int32(len(titles))}, exportMetadata(t, done))
	rows := exportedBooks(t, done)[1:]
	exportedTitles := make([]string, len(rows))
	for i, row := range rows {
		exportedTitles[i] = row[1]
	}
	require.ElementsMatch(t, titles, exportedTitles)
}

func TestExportBooks_Reject(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(2)
	kept := createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[0])
	rejected := createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), titles[1])

	done := exportBooks(t, fixture.shelf.GetName(), "", titles[1])
	metadata := exportMetadata(t, done)
	require.Len(t, metadata.GetErrors(), 1)
	require.Equal(t, int32(codes.InvalidArgument), metadata.GetErrors()[0].GetCode())
	require.Equal(t, fmt.Sprintf("book %q is rejected", rejected.GetName()), metadata.GetErrors()[0].GetMessage())
	metadata.Errors = nil
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 1, FailureCount: 1}, metadata)
	require.Equal(t, [][]string{{"name", "title"}, {kept.GetName(), titles[0]}}, exportedBooks(t, done))
}

// The destination selects the runner method: titles land in the response, not a CSV.
func TestExportBooks_TitlesDestination(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(2)
	for _, title := range titles {
		createTestBook(t, fixture.shelf.GetName(), fixture.author.GetName(), title)
	}

	done := runExportBooks(t, &libraryservicepb.ExportBooksRequest{
		Parent:      fixture.shelf.GetName(),
		Destination: &libraryservicepb.ExportBooksRequest_TitlesDestination{TitlesDestination: &libraryservicepb.TitlesDestination{}},
		RequestId:   uuid.MustNewV7().String(),
	})
	grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: 2}, exportMetadata(t, done))
	response := unpackAny[*libraryservicepb.ExportBooksResponse](t, done.GetResponse())
	require.Empty(t, response.GetCsv())
	require.ElementsMatch(t, titles, response.GetTitles())
}

func TestExportBooks_MissingDestination(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	request := &libraryservicepb.ExportBooksRequest{Parent: fixture.shelf.GetName(), RequestId: uuid.MustNewV7().String()}
	_, err := libraryServiceClient.ExportBooks(ctx, request)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestExportShelves(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	deleted := createTestShelf(t, fixture.organization, "Deleted Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: deleted.GetName()}
	_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
	require.NoError(t, err)

	exportShelves := func(showDeleted bool) [][]string {
		t.Helper()
		exportShelvesRequest := &libraryservicepb.ExportShelvesRequest{
			Parent:      fixture.organization,
			ShowDeleted: showDeleted,
			Destination: &libraryservicepb.ExportShelvesRequest_CsvDestination{CsvDestination: &libraryservicepb.CsvDestination{}},
			RequestId:   uuid.MustNewV7().String(),
		}
		operation, err := libraryServiceClient.ExportShelves(ctx, exportShelvesRequest)
		require.NoError(t, err)
		done := waitOperation(t, operation.GetName(), operationWaitTimeout)
		require.Nil(t, done.GetError())
		rows := csvRows(t, unpackAny[*libraryservicepb.ExportShelvesResponse](t, done.GetResponse()).GetCsv())
		grpcrequire.Equal(t, &aippb.ExportMetadata{SuccessCount: int32(len(rows) - 1)}, exportMetadata(t, done))
		return rows
	}

	// expected is the export of the organization's shelves as ListShelves has them.
	expected := func(showDeleted bool) [][]string {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{Parent: fixture.organization, ShowDeleted: showDeleted, PageSize: 1000}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		rows := [][]string{{"name", "display_name"}}
		for _, shelf := range listShelvesResponse.GetShelves() {
			rows = append(rows, []string{shelf.GetName(), shelf.GetDisplayName()})
		}
		slices.SortFunc(rows[1:], func(a, b []string) int { return strings.Compare(a[0], b[0]) })
		return rows
	}

	live := expected(false)
	require.Len(t, live, 2)
	require.Equal(t, live, exportShelves(false))

	all := expected(true)
	require.Len(t, all, 3)
	require.Equal(t, all, exportShelves(true))
}
