package library_service

import (
	"context"
	"encoding/csv"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/gengo/test/library/library_service/rpc"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// RunExportBooks writes ExportBooks' CSV: one `name,title` row per book.
func (s *Service) RunExportBooks(ctx context.Context, request *libraryservicepb.ExportBooksRequest, reader *rpc.ExportBooksReader) (*libraryservicepb.ExportBooksResponse, error) {
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
			if request.GetRejectTitle() != "" && book.GetTitle() == request.GetRejectTitle() {
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

// RunExportShelves writes ExportShelves' CSV: one `name,display_name` row per shelf.
func (s *Service) RunExportShelves(ctx context.Context, request *libraryservicepb.ExportShelvesRequest, reader *rpc.ExportShelvesReader) (*libraryservicepb.ExportShelvesResponse, error) {
	var builder strings.Builder
	writer := csv.NewWriter(&builder)
	if err := writer.Write([]string{"name", "display_name"}); err != nil {
		return nil, status.Errorf(codes.Internal, "writing csv header: %v", err).Err()
	}
	for {
		shelves, err := reader.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(shelves) == 0 {
			break
		}
		for _, shelf := range shelves {
			if err := writer.Write([]string{shelf.GetName(), shelf.GetDisplayName()}); err != nil {
				return nil, status.Errorf(codes.Internal, "writing csv row: %v", err).Err()
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "flushing csv: %v", err).Err()
	}
	return &libraryservicepb.ExportShelvesResponse{Csv: builder.String()}, nil
}
