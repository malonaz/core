package library_service

import (
	"context"
	"fmt"

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
		return len(metadata.ValueFromIncomingContext(ctx, MetadataKeyEnableHook)) > 0
	}

	if err := middleware.RegisterHookHandler(func(ctx context.Context, author *pb.Author) error {
		aip.SetLabel(author, "hook/author-response", aip.LabelValueTrue)
		return nil
	}, middleware.WithHookOnResponse(), middleware.WithHookMatchers(matcher)); err != nil {
		return nil, fmt.Errorf("registering author hook handler: %w", err)
	}

	if err := middleware.RegisterHookHandler(func(ctx context.Context, book *pb.Book) error {
		aip.SetLabel(book, "hook/book-request", aip.LabelValueTrue)
		return nil
	}, middleware.WithHookOnRequest(), middleware.WithHookMatchers(matcher)); err != nil {
		return nil, fmt.Errorf("registering book request hook handler: %w", err)
	}

	if err := middleware.RegisterHookHandler(func(ctx context.Context, book *pb.Book) error {
		aip.SetLabel(book, "hook/book-response", aip.LabelValueTrue)
		return nil
	}, middleware.WithHookOnResponse(), middleware.WithHookMatchers(matcher)); err != nil {
		return nil, fmt.Errorf("registering book response hook handler: %w", err)
	}

	if err := middleware.RegisterHookHandler(func(ctx context.Context, shelfMetadata *pb.ShelfMetadata) error {
		shelfMetadata.Dummy = "hello"
		return nil
	}, middleware.WithHookOnRequest(), middleware.WithHookMatchers(matcher, middleware.MatchMethods("CreateShelf"))); err != nil {
		return nil, fmt.Errorf("registering shelf metadata hook handler: %w", err)
	}

	if err := middleware.RegisterHookHandler(func(ctx context.Context, shelfNote *pb.ShelfNote) error {
		shelfNote.Content = shelfNote.Content + " [hooked]"
		return nil
	}, middleware.WithHookOnRequest(), middleware.WithHookMatchers(matcher)); err != nil {
		return nil, fmt.Errorf("registering shelf note hook handler: %w", err)
	}

	return func() {}, nil
}
