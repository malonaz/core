package library_service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/malonaz/core/gengo/test/library/library_service/rpc"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// importedBookNamespace derives a book's ID from its title, so a retried import
// finds the books an earlier attempt created instead of duplicating them.
var importedBookNamespace = uuid.MustParse("3c1a6d0e-7b52-4f1e-9a8c-2d4f6e8a0b1c")

// ImportBooksFromTitles is ImportBooks' titles source: one book per title, one
// at a time so progress and cancellation can be observed.
func (s *Service) ImportBooksFromTitles(ctx context.Context, request *libraryservicepb.ImportBooksRequest, sink *rpc.ImportBooksSink) error {
	source := request.GetTitlesSource()
	if err := sink.SetTotal(ctx, int32(len(source.GetTitles()))); err != nil {
		return err
	}
	shelf, err := librarypb.ParseShelfRn(request.GetParent())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	imported := 0
	for _, title := range source.GetTitles() {
		if err := wait(ctx, source.GetDelay().AsDuration()); err != nil {
			return err
		}
		book := &librarypb.Book{
			Name:   shelf.BookRn(aip.NewDeterministicBase32ResourceID(importedBookNamespace, title)).String(),
			Title:  title,
			Author: source.GetAuthor(),
			// The book table stores duration and metadata in non-null columns.
			Duration: durationpb.New(0),
			Metadata: &librarypb.BookMetadata{},
		}
		// The sink cannot tell a fresh insert from a replay; only a fresh one trips
		// the hook, so the retry finds the book and carries on.
		fresh, err := s.bookMissing(ctx, book.GetName())
		if err != nil {
			return err
		}
		books, err := sink.Import(ctx, []*librarypb.Book{book})
		if err != nil {
			return err
		}
		imported += len(books)
		if fresh && imported == int(source.GetFailAfter()) {
			return status.Errorf(codes.Internal, "failing after %d books as requested", source.GetFailAfter()).Err()
		}
	}
	return nil
}

// bookMissing reports whether no book of that name exists yet.
func (s *Service) bookMissing(ctx context.Context, name string) (bool, error) {
	getBookRequest := &libraryservicepb.GetBookRequest{Name: name}
	if _, err := s.LibraryServiceServer.GetBook(ctx, getBookRequest); err != nil {
		if status.HasCode(err, codes.NotFound) {
			return true, nil
		}
		return false, err
	}
	return false, nil
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
