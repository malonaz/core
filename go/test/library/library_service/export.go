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
)

// ExportBooksToCsv writes ExportBooks' CSV: one `name,title` row per book.
func (s *Service) ExportBooksToCsv(ctx context.Context, request *libraryservicepb.ExportBooksRequest, reader *rpc.ExportBooksReader) (*libraryservicepb.CsvResult, error) {
	row := func(ctx context.Context, book *librarypb.Book) ([]string, error) {
		if request.GetRejectTitle() != "" && book.GetTitle() == request.GetRejectTitle() {
			return nil, reader.Fail(ctx, status.Errorf(codes.InvalidArgument, "book %q is rejected", book.GetName()).Err())
		}
		return []string{book.GetName(), book.GetTitle()}, nil
	}
	document, err := writeCsv(ctx, []string{"name", "title"}, reader.Next, row)
	if err != nil {
		return nil, err
	}
	return &libraryservicepb.CsvResult{Csv: document}, nil
}

// ExportBooksToTitles writes ExportBooks' titles: one per book.
func (s *Service) ExportBooksToTitles(ctx context.Context, request *libraryservicepb.ExportBooksRequest, reader *rpc.ExportBooksReader) (*libraryservicepb.TitlesResult, error) {
	var titles []string
	for {
		books, err := reader.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(books) == 0 {
			return &libraryservicepb.TitlesResult{Titles: titles}, nil
		}
		for _, book := range books {
			titles = append(titles, book.GetTitle())
		}
	}
}

// ExportShelvesToCsv writes ExportShelves' CSV: one `name,display_name` row per shelf.
func (s *Service) ExportShelvesToCsv(ctx context.Context, request *libraryservicepb.ExportShelvesRequest, reader *rpc.ExportShelvesReader) (*libraryservicepb.CsvResult, error) {
	row := func(_ context.Context, shelf *librarypb.Shelf) ([]string, error) {
		return []string{shelf.GetName(), shelf.GetDisplayName()}, nil
	}
	document, err := writeCsv(ctx, []string{"name", "display_name"}, reader.Next, row)
	if err != nil {
		return nil, err
	}
	return &libraryservicepb.CsvResult{Csv: document}, nil
}

// writeCsv drains next into a CSV document: the header, then a row per resource. A resource
// row returns no record for is left out.
func writeCsv[T any](
	ctx context.Context, header []string, next func(context.Context) ([]T, error), row func(context.Context, T) ([]string, error),
) (string, error) {
	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.Write(header); err != nil {
		return "", status.Errorf(codes.Internal, "writing csv header: %v", err).Err()
	}
	for {
		resources, err := next(ctx)
		if err != nil {
			return "", err
		}
		if len(resources) == 0 {
			break
		}
		for _, resource := range resources {
			record, err := row(ctx, resource)
			if err != nil {
				return "", err
			}
			if record == nil {
				continue
			}
			if err := writer.Write(record); err != nil {
				return "", status.Errorf(codes.Internal, "writing csv row: %v", err).Err()
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", status.Errorf(codes.Internal, "flushing csv: %v", err).Err()
	}
	return builder.String(), nil
}
