package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	libraryv1 "github.com/malonaz/core/genproto/test/library/v1"
)

const (
	createBookFullMethod = "/malonaz.test.library.library_service.v1.LibraryService/CreateBook"
	listBooksFullMethod  = "/malonaz.test.library.library_service.v1.LibraryService/ListBooks"
)

func maskContext(pairs ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
}

func newTestBook(id string) *libraryv1.Book {
	return &libraryv1.Book{Name: "shelves/1/books/" + id, Title: "Title " + id, Metadata: &libraryv1.BookMetadata{Summary: "summary"}}
}

func TestUnaryServerFieldMask(t *testing.T) {
	interceptor := UnaryServerFieldMask()
	invoke := func(ctx context.Context, fullMethod string, response proto.Message) (any, bool, error) {
		var called bool
		handler := func(context.Context, any) (any, error) {
			called = true
			return response, nil
		}
		result, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
		return result, called, err
	}

	t.Run("an invalid strict read mask fails before the handler runs", func(t *testing.T) {
		_, called, err := invoke(maskContext(metadataKeyReadMaskStrict, "name,subject"), createBookFullMethod, newTestBook("1"))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "invalid read mask")
		require.False(t, called)
	})

	t.Run("an invalid strict field mask fails before the handler runs", func(t *testing.T) {
		_, called, err := invoke(maskContext(metadataKeyFieldMaskStrict, "books.subject"), listBooksFullMethod, &librarypb.ListBooksResponse{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "invalid field mask")
		require.False(t, called)
	})

	t.Run("a strict read mask is validated against the listed resource, not the envelope", func(t *testing.T) {
		response := &librarypb.ListBooksResponse{Books: []*libraryv1.Book{newTestBook("1"), newTestBook("2")}, NextPageToken: "next"}
		result, called, err := invoke(maskContext(metadataKeyReadMaskStrict, "title,metadata.summary"), listBooksFullMethod, response)
		require.NoError(t, err)
		require.True(t, called)
		expected := &librarypb.ListBooksResponse{
			Books: []*libraryv1.Book{
				{Title: "Title 1", Metadata: &libraryv1.BookMetadata{Summary: "summary"}},
				{Title: "Title 2", Metadata: &libraryv1.BookMetadata{Summary: "summary"}},
			},
			NextPageToken: "next",
		}
		require.True(t, proto.Equal(expected, result.(proto.Message)), result)
	})

	t.Run("envelope paths are rejected even when the list is empty", func(t *testing.T) {
		_, called, err := invoke(maskContext(metadataKeyReadMaskStrict, "books"), listBooksFullMethod, &librarypb.ListBooksResponse{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.False(t, called)
	})

	t.Run("a lenient read mask ignores unknown paths", func(t *testing.T) {
		result, called, err := invoke(maskContext(metadataKeyReadMask, "title,subject"), createBookFullMethod, newTestBook("1"))
		require.NoError(t, err)
		require.True(t, called)
		require.True(t, proto.Equal(&libraryv1.Book{Title: "Title 1"}, result.(proto.Message)), result)
	})

	t.Run("a wildcard returns the whole response", func(t *testing.T) {
		result, called, err := invoke(maskContext(metadataKeyReadMaskStrict, "*"), createBookFullMethod, newTestBook("1"))
		require.NoError(t, err)
		require.True(t, called)
		require.True(t, proto.Equal(newTestBook("1"), result.(proto.Message)), result)
	})
}

type testServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []any
}

func (s *testServerStream) Context() context.Context { return s.ctx }

func (s *testServerStream) SendMsg(m any) error {
	s.sent = append(s.sent, m)
	return nil
}

func TestStreamServerFieldMask(t *testing.T) {
	interceptor := StreamServerFieldMask()
	info := &grpc.StreamServerInfo{FullMethod: createBookFullMethod, IsServerStream: true}

	t.Run("an invalid strict read mask fails before the handler runs", func(t *testing.T) {
		var called bool
		stream := &testServerStream{ctx: maskContext(metadataKeyReadMaskStrict, "subject")}
		err := interceptor(nil, stream, info, func(any, grpc.ServerStream) error {
			called = true
			return nil
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.False(t, called)
	})

	t.Run("a valid strict read mask prunes every sent message", func(t *testing.T) {
		stream := &testServerStream{ctx: maskContext(metadataKeyReadMaskStrict, "name")}
		err := interceptor(nil, stream, info, func(_ any, stream grpc.ServerStream) error {
			return stream.SendMsg(newTestBook("1"))
		})
		require.NoError(t, err)
		require.Len(t, stream.sent, 1)
		require.True(t, proto.Equal(&libraryv1.Book{Name: "shelves/1/books/1"}, stream.sent[0].(proto.Message)))
	})
}
