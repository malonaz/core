package sat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/scheduler/longrunning"
	"github.com/malonaz/core/go/uuid"
)

// Generous ceiling for polling assertions.
const operationWaitTimeout = 30 * time.Second

// importFixture is a shelf and an author to import books onto.
type importFixture struct {
	organization string
	shelf        *librarypb.Shelf
	author       *librarypb.Author
}

func newImportFixture(t *testing.T) *importFixture {
	t.Helper()
	organization := getOrganizationParent()
	return &importFixture{
		organization: organization,
		shelf:        createTestShelf(t, organization, "Import Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION),
		author:       createTestAuthor(t, organization, "Import Author"),
	}
}

// titles returns count unique titles, so imports never collide across tests.
func titles(count int) []string {
	run := uuid.MustNewV7().String()
	titles := make([]string, count)
	for i := range titles {
		titles[i] = fmt.Sprintf("%s-%02d", run, i)
	}
	return titles
}

// importBooks starts an import and returns its not-done operation.
func (f *importFixture) importBooks(t *testing.T, request *libraryservicepb.ImportBooksRequest) *longrunningpb.Operation {
	t.Helper()
	request.Parent = f.shelf.GetName()
	request.Author = f.author.GetName()
	operation, err := libraryServiceClient.ImportBooks(ctx, request)
	require.NoError(t, err)
	require.Regexp(t, "^"+f.organization+"/operations/[a-z0-9]+$", operation.GetName())
	require.False(t, operation.GetDone())
	require.Nil(t, operation.GetResult())
	return operation
}

// books returns the shelf's books.
func (f *importFixture) books(t *testing.T) []*librarypb.Book {
	t.Helper()
	listBooksRequest := &libraryservicepb.ListBooksRequest{Parent: f.shelf.GetName(), PageSize: 1000}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	return listBooksResponse.GetBooks()
}

func getOperation(t *testing.T, name string) *longrunningpb.Operation {
	t.Helper()
	getOperationRequest := &longrunningpb.GetOperationRequest{Name: name}
	operation, err := operationsClient.GetOperation(ctx, getOperationRequest)
	require.NoError(t, err)
	return operation
}

// waitOperation calls WaitOperation under a client deadline wide enough for
// the server cap, not the client, to end the wait.
func waitOperation(t *testing.T, name string, timeout time.Duration) *longrunningpb.Operation {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, operationWaitTimeout)
	defer cancel()
	waitOperationRequest := &longrunningpb.WaitOperationRequest{Name: name, Timeout: durationpb.New(timeout)}
	operation, err := operationsClient.WaitOperation(callCtx, waitOperationRequest)
	require.NoError(t, err)
	return operation
}

// waitForOperation polls the operation until predicate holds and returns it.
func waitForOperation(t *testing.T, name string, predicate func(*longrunningpb.Operation) bool) *longrunningpb.Operation {
	t.Helper()
	var operation *longrunningpb.Operation
	require.Eventually(t, func() bool {
		operation = getOperation(t, name)
		return predicate(operation)
	}, operationWaitTimeout, 20*time.Millisecond, "operation %s never matched", name)
	return operation
}

func listOperations(t *testing.T, name, filter string) []string {
	t.Helper()
	listOperationsRequest := &longrunningpb.ListOperationsRequest{Name: name, Filter: filter}
	listOperationsResponse, err := operationsClient.ListOperations(ctx, listOperationsRequest)
	require.NoError(t, err)
	names := make([]string, len(listOperationsResponse.GetOperations()))
	for i, operation := range listOperationsResponse.GetOperations() {
		names[i] = operation.GetName()
	}
	return names
}

// unpackAny unmarshals an Any into M.
func unpackAny[M proto.Message](t *testing.T, payload *anypb.Any) M {
	t.Helper()
	var message M
	message = message.ProtoReflect().New().Interface().(M)
	require.NoError(t, payload.UnmarshalTo(message))
	return message
}

func importMetadata(t *testing.T, operation *longrunningpb.Operation) *libraryservicepb.ImportBooksMetadata {
	t.Helper()
	require.NotNil(t, operation.GetMetadata(), "operation %s has no metadata", operation.GetName())
	return unpackAny[*libraryservicepb.ImportBooksMetadata](t, operation.GetMetadata())
}

// setImportMaxAttempts changes the shared queue's retry budget; tests relying
// on it run sequentially.
func setImportMaxAttempts(t *testing.T, maxAttempts int32) {
	t.Helper()
	updateQueueRequest := &schedulerservicepb.UpdateQueueRequest{
		Queue:      &schedulerpb.Queue{Name: queueOf(t, "ImportBooks").GetName(), Policy: &policypb.QueuePolicy{MaxAttempts: maxAttempts}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"policy.max_attempts"}},
	}
	_, err := schedulerServiceClient.UpdateQueue(ctx, updateQueueRequest)
	require.NoError(t, err)
}

func TestImportBooks_Lifecycle(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(5)
	operation := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles, Delay: durationpb.New(300 * time.Millisecond)})

	// Progress is observable while the import runs.
	running := waitForOperation(t, operation.GetName(), func(operation *longrunningpb.Operation) bool {
		return operation.GetMetadata() != nil && importMetadata(t, operation).GetImported() > 0
	})
	require.False(t, running.GetDone())
	metadata := importMetadata(t, running)
	require.Equal(t, int32(len(titles)), metadata.GetTotal())
	require.Less(t, metadata.GetImported(), metadata.GetTotal())

	done := waitOperation(t, operation.GetName(), operationWaitTimeout)
	require.True(t, done.GetDone())
	require.Nil(t, done.GetError())
	metadata = importMetadata(t, done)
	require.Equal(t, int32(len(titles)), metadata.GetImported())
	require.Equal(t, int32(len(titles)), metadata.GetTotal())

	response := unpackAny[*libraryservicepb.ImportBooksResponse](t, done.GetResponse())
	require.Len(t, response.GetBooks(), len(titles))
	for i, book := range response.GetBooks() {
		require.Equal(t, titles[i], book.GetTitle())
		require.Equal(t, fixture.author.GetName(), book.GetAuthor())
	}
	require.Len(t, fixture.books(t), len(titles))
	grpcrequire.Equal(t, done, getOperation(t, operation.GetName()))
}

func TestImportBooks_WaitOperation(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	operation := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles(20), Delay: durationpb.New(500 * time.Millisecond)})
	t.Cleanup(func() {
		cancelOperationRequest := &longrunningpb.CancelOperationRequest{Name: operation.GetName()}
		_, _ = operationsClient.CancelOperation(ctx, cancelOperationRequest)
	})

	t.Run("short timeout returns the running operation", func(t *testing.T) {
		start := time.Now()
		waited := waitOperation(t, operation.GetName(), 300*time.Millisecond)
		require.Less(t, time.Since(start), schedulerWaitJobMaxTimeout)
		require.False(t, waited.GetDone())
		require.Equal(t, operation.GetName(), waited.GetName())
	})

	t.Run("absurd timeout is capped", func(t *testing.T) {
		start := time.Now()
		waited := waitOperation(t, operation.GetName(), time.Hour)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed, schedulerWaitJobMaxTimeout)
		require.Less(t, elapsed, 2*schedulerWaitJobMaxTimeout)
		require.False(t, waited.GetDone())
	})
}

func TestImportBooks_ListOperations(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	otherShelf := createTestShelf(t, fixture.organization, "Other Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	first := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles(2)})
	second := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles(20), Delay: durationpb.New(500 * time.Millisecond)})
	t.Cleanup(func() {
		cancelOperationRequest := &longrunningpb.CancelOperationRequest{Name: second.GetName()}
		_, _ = operationsClient.CancelOperation(ctx, cancelOperationRequest)
	})
	otherRequest := &libraryservicepb.ImportBooksRequest{Parent: otherShelf.GetName(), Author: fixture.author.GetName(), Titles: titles(1)}
	other, err := libraryServiceClient.ImportBooks(ctx, otherRequest)
	require.NoError(t, err)

	// A job of another method under the same organization: not this service's,
	// so invisible through its Operations server.
	foreign, err := scheduler.CreateJob(ctx, schedulerServiceClient, fixture.organization, &libraryservicepb.GetShelfRequest{Name: fixture.shelf.GetName()},
		scheduler.WithScheduleTime(time.Now().Add(24*time.Hour)))
	require.NoError(t, err)
	t.Cleanup(func() {
		cancelJobRequest := &schedulerservicepb.CancelJobRequest{Name: foreign.GetName()}
		_, _ = schedulerServiceClient.CancelJob(ctx, cancelJobRequest)
	})
	foreignName, err := longrunning.OperationName(foreign.GetName())
	require.NoError(t, err)

	waitOperation(t, first.GetName(), operationWaitTimeout)
	waitOperation(t, other.GetName(), operationWaitTimeout)

	// Every import of the organization, whichever shelf; not the foreign job, not another organization's.
	require.ElementsMatch(t, []string{first.GetName(), second.GetName(), other.GetName()}, listOperations(t, fixture.organization, ""))
	require.Empty(t, listOperations(t, getOrganizationParent(), ""))

	require.ElementsMatch(t, []string{first.GetName(), other.GetName()}, listOperations(t, fixture.organization, "done"))
	require.ElementsMatch(t, []string{first.GetName(), other.GetName()}, listOperations(t, fixture.organization, "done = true"))
	require.Equal(t, []string{second.GetName()}, listOperations(t, fixture.organization, "NOT done"))
	require.Equal(t, []string{second.GetName()}, listOperations(t, fixture.organization, "-done"))

	listOperationsRequest := &longrunningpb.ListOperationsRequest{Name: fixture.organization, Filter: `name = "x"`}
	_, err = operationsClient.ListOperations(ctx, listOperationsRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)

	getOperationRequest := &longrunningpb.GetOperationRequest{Name: foreignName}
	_, err = operationsClient.GetOperation(ctx, getOperationRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestImportBooks_Cancel(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	operation := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles(20), Delay: durationpb.New(300 * time.Millisecond)})
	waitForOperation(t, operation.GetName(), func(operation *longrunningpb.Operation) bool {
		return operation.GetMetadata() != nil && importMetadata(t, operation).GetImported() > 0
	})

	cancelOperationRequest := &longrunningpb.CancelOperationRequest{Name: operation.GetName()}
	_, err := operationsClient.CancelOperation(ctx, cancelOperationRequest)
	require.NoError(t, err)
	cancelled := getOperation(t, operation.GetName())
	require.True(t, cancelled.GetDone())
	require.Equal(t, int32(codes.Canceled), cancelled.GetError().GetCode())

	// The runner's call was cut short: no more books appear.
	time.Sleep(time.Second)
	imported := len(fixture.books(t))
	require.Less(t, imported, 20)
	time.Sleep(time.Second)
	require.Len(t, fixture.books(t), imported)

	_, err = operationsClient.CancelOperation(ctx, cancelOperationRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)

	deleteOperationRequest := &longrunningpb.DeleteOperationRequest{Name: operation.GetName()}
	_, err = operationsClient.DeleteOperation(ctx, deleteOperationRequest)
	require.NoError(t, err)
	getOperationRequest := &longrunningpb.GetOperationRequest{Name: operation.GetName()}
	_, err = operationsClient.GetOperation(ctx, getOperationRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestImportBooks_Retry(t *testing.T) {
	// Not parallel: toggles the shared queue's retry budget.
	fixture := newImportFixture(t)

	t.Run("second attempt completes without duplicates", func(t *testing.T) {
		titles := titles(4)
		operation := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles, FailAfter: 2})
		done := waitOperation(t, operation.GetName(), operationWaitTimeout)
		require.True(t, done.GetDone())
		require.Nil(t, done.GetError())
		require.Len(t, unpackAny[*libraryservicepb.ImportBooksResponse](t, done.GetResponse()).GetBooks(), len(titles))
		require.Equal(t, int32(len(titles)), importMetadata(t, done).GetImported())
		require.Len(t, fixture.books(t), len(titles))
	})

	t.Run("single attempt fails the operation", func(t *testing.T) {
		setImportMaxAttempts(t, 1)
		t.Cleanup(func() { setImportMaxAttempts(t, 2) })
		operation := fixture.importBooks(t, &libraryservicepb.ImportBooksRequest{Titles: titles(3), FailAfter: 1})
		done := waitOperation(t, operation.GetName(), operationWaitTimeout)
		require.True(t, done.GetDone())
		require.Equal(t, int32(codes.Internal), done.GetError().GetCode())
		require.Nil(t, done.GetResponse())
	})
}

func TestImportBooks_RequestID(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	request := &libraryservicepb.ImportBooksRequest{Titles: titles(1), RequestId: uuid.MustNewV7().String()}
	first := fixture.importBooks(t, request)
	repeatedRequest := &libraryservicepb.ImportBooksRequest{Parent: fixture.shelf.GetName(), Author: fixture.author.GetName(), Titles: request.GetTitles(), RequestId: request.GetRequestId()}
	repeated, err := libraryServiceClient.ImportBooks(ctx, repeatedRequest)
	require.NoError(t, err)
	require.Equal(t, first.GetName(), repeated.GetName())
	require.Len(t, listOperations(t, fixture.organization, ""), 1)
}

func TestImportBooks_RunInline(t *testing.T) {
	t.Parallel()
	fixture := newImportFixture(t)
	titles := titles(2)

	// The scheduler's job metadata selects the runner: the import happens in
	// the call, which is how a runner is exercised directly.
	jobID := uuid.MustNewV7().String()
	runCtx := metadata.AppendToOutgoingContext(ctx, scheduler.JobMetadataKey, "jobs/"+jobID)
	importBooksRequest := &libraryservicepb.ImportBooksRequest{Parent: fixture.shelf.GetName(), Author: fixture.author.GetName(), Titles: titles}
	operation, err := libraryServiceClient.ImportBooks(runCtx, importBooksRequest)
	require.NoError(t, err)
	require.Equal(t, "operations/"+jobID, operation.GetName())
	require.True(t, operation.GetDone())
	require.Len(t, unpackAny[*libraryservicepb.ImportBooksResponse](t, operation.GetResponse()).GetBooks(), len(titles))
	require.Len(t, fixture.books(t), len(titles))
}
