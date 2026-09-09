package library_service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler/longrunning"
	"github.com/malonaz/core/go/uuid"
)

// importedBookNamespace derives a book's ID from its title, so a retried import
// finds the books an earlier attempt created instead of duplicating them.
var importedBookNamespace = uuid.MustParse("3c1a6d0e-7b52-4f1e-9a8c-2d4f6e8a0b1c")

// RunImportBooks is the runner of ImportBooks: it creates one book per title,
// reporting progress after each, and returns them all.
func (s *Service) RunImportBooks(ctx context.Context, request *libraryservicepb.ImportBooksRequest) (*libraryservicepb.ImportBooksResponse, error) {
	total := int32(len(request.GetTitles()))
	books := make([]*librarypb.Book, 0, total)
	for _, title := range request.GetTitles() {
		if err := wait(ctx, request.GetDelay().AsDuration()); err != nil {
			return nil, err
		}
		book, created, err := s.importBook(ctx, request, title)
		if err != nil {
			return nil, err
		}
		books = append(books, book)
		// Only a fresh import trips the hook, so the retry finds the book and carries on.
		if created && int32(len(books)) == request.GetFailAfter() {
			return nil, status.Errorf(codes.Internal, "failing after %d books as requested", request.GetFailAfter()).Err()
		}
		metadata := &libraryservicepb.ImportBooksMetadata{Imported: int32(len(books)), Total: total}
		if err := longrunning.ReportProgress(ctx, s.schedulerServiceClient, metadata); err != nil {
			// The job left RUNNING (cancelled or reaped): stop. Any other failure is a lost progress update, not a lost import.
			if status.HasCode(err, codes.FailedPrecondition) {
				return nil, err
			}
			s.log.WarnContext(ctx, "reporting import progress", "error", err)
		}
	}
	return &libraryservicepb.ImportBooksResponse{Books: books}, nil
}

// importBook creates the titled book, or returns it when an earlier attempt
// already did, reporting which.
func (s *Service) importBook(ctx context.Context, request *libraryservicepb.ImportBooksRequest, title string) (*librarypb.Book, bool, error) {
	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: request.GetParent(),
		BookId: aip.NewDeterministicBase32ResourceID(importedBookNamespace, title),
		// The book table stores duration and metadata in non-null columns.
		Book: &librarypb.Book{Title: title, Author: request.GetAuthor(), Duration: durationpb.New(0), Metadata: &librarypb.BookMetadata{}},
	}
	book, err := s.LibraryServiceServer.CreateBook(ctx, createBookRequest)
	if err == nil {
		return book, true, nil
	}
	if !status.HasCode(err, codes.AlreadyExists) {
		return nil, false, err
	}
	shelf := &librarypb.ShelfResourceName{}
	if err := shelf.UnmarshalString(request.GetParent()); err != nil {
		return nil, false, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	getBookRequest := &libraryservicepb.GetBookRequest{Name: shelf.BookResourceName(createBookRequest.GetBookId()).String()}
	book, err = s.LibraryServiceServer.GetBook(ctx, getBookRequest)
	return book, false, err
}

// wait sleeps for the duration unless the call is cancelled first.
func wait(ctx context.Context, duration time.Duration) error {
	if duration == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return status.FromError(ctx.Err(), "waiting between imports").Err()
	case <-time.After(duration):
		return nil
	}
}
