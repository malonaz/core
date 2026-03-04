package library_service

import (
	"context"

	"google.golang.org/grpc/metadata"

	pb "github.com/malonaz/core/genproto/library/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/middleware"
)

const (
	MetadataKeyEnableHook = "x-enable-hook"
)

type Opts struct{}

type runtime struct{}

func newRuntime(opts *Opts) (*runtime, error) {
	return &runtime{}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	// Base matcher.
	matcher := func(ctx context.Context, callMetadata *middleware.CallMetadata) bool {
		if len(metadata.ValueFromIncomingContext(ctx, MetadataKeyEnableHook)) == 0 {
			return false
		}
		return true
	}

	middleware.RegisterHookHandler("library.malonaz.com/Author", func(ctx context.Context, author *pb.Author) error {
		aip.SetLabel(author, "hook/author-response", aip.LabelValueTrue)
		return nil
	}, middleware.WithMatcher(matcher))

	middleware.RegisterHookHandler("library.malonaz.com/Book", func(ctx context.Context, book *pb.Book) error {
		aip.SetLabel(book, "hook/book-request", aip.LabelValueTrue)
		return nil
	}, middleware.WithMatcher(matcher))

	middleware.RegisterHookHandler("library.malonaz.com/Book", func(ctx context.Context, book *pb.Book) error {
		aip.SetLabel(book, "hook/book-response", aip.LabelValueTrue)
		return nil
	}, middleware.WithMatcher(matcher))

	middleware.RegisterHookHandler("library.malonaz.com/ShelfMetadata", func(ctx context.Context, shelfMetadata *pb.ShelfMetadata) error {
		shelfMetadata.Dummy = "hello"
		return nil
	}, middleware.WithMatcher(func(ctx context.Context, callMetadata *middleware.CallMetadata) bool {
		return matcher(ctx, callMetadata) && callMetadata.Method == "CreateShelf"
	}))

	return func() {}, nil
}
