package library_service

import (
	"context"
	"encoding/csv"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/gengo/test/library/library_service/rpc"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/postgres"
)

// AggregateExportShelves attaches its books to each shelf of a page, in one
// query for the whole page.
func (s *Service) AggregateExportShelves(ctx context.Context, request *libraryservicepb.ExportShelvesRequest, shelves []*librarypb.Shelf) ([]*libraryservicepb.ExportedShelf, error) {
	shelfIDs := make([]string, len(shelves))
	for i, shelf := range shelves {
		shelfRn, err := librarypb.ParseShelfRn(shelf.GetName())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parsing shelf name: %v", err).Err()
		}
		shelfIDs[i] = shelfRn.Shelf
	}
	// Shelf IDs may repeat across organizations: the query over-fetches, the
	// grouping by shelf name keeps each book on its own shelf.
	whereClause := postgres.AddToWhereClause("", "book.shelf_id = ANY($1)")
	dbBooks, err := s.libraryPostgresStore.ListBooks(ctx, "-", "-", whereClause, "ORDER BY book.book_id", "", nil, shelfIDs)
	if err != nil {
		return nil, status.FromError(err, "listing books").Err()
	}
	shelfNameToBooks := map[string][]*librarypb.Book{}
	for _, dbBook := range dbBooks {
		book, err := dbBook.ToPb()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting book from model to pb: %v", err).Err()
		}
		shelfName := (&librarypb.ShelfRn{Organization: dbBook.OrganizationID, Shelf: dbBook.ShelfID}).String()
		shelfNameToBooks[shelfName] = append(shelfNameToBooks[shelfName], book)
	}

	exportedShelves := make([]*libraryservicepb.ExportedShelf, len(shelves))
	for i, shelf := range shelves {
		exportedShelves[i] = &libraryservicepb.ExportedShelf{Shelf: shelf, Books: shelfNameToBooks[shelf.GetName()]}
	}
	return exportedShelves, nil
}

// ExportBooksToCsv is ExportBooks' CSV destination: one `name,title` row per book.
func (s *Service) ExportBooksToCsv(ctx context.Context, request *libraryservicepb.ExportBooksRequest, reader *rpc.ExportBooksReader) (*libraryservicepb.ExportBooksResponse, error) {
	destination := request.GetCsvDestination()
	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.Write([]string{"name", "title"}); err != nil {
		return nil, status.Errorf(codes.Internal, "writing csv header: %v", err).Err()
	}
	for {
		books, err := reader.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(books) == 0 {
			break
		}
		for _, book := range books {
			if destination.GetRejectTitle() != "" && book.GetTitle() == destination.GetRejectTitle() {
				if err := reader.Fail(ctx, status.Errorf(codes.InvalidArgument, "book %q is rejected", book.GetName()).Err()); err != nil {
					return nil, err
				}
				continue
			}
			if err := writer.Write([]string{book.GetName(), book.GetTitle()}); err != nil {
				return nil, status.Errorf(codes.Internal, "writing csv row: %v", err).Err()
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "flushing csv: %v", err).Err()
	}
	return &libraryservicepb.ExportBooksResponse{Csv: builder.String()}, nil
}
