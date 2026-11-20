package authentication_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
)

// BenchmarkServerToken measures what a call between servers adds: minting the token, and verifying it.
func BenchmarkServerToken(b *testing.B) {
	f := newFixture(b)
	interceptor := f.interceptor(b)
	require.NoError(b, interceptor.Serve("gateway", mockService))
	// A handler's context after a token holder's call to the gateway, with the request info gRPC gives credentials.
	var handlerContext context.Context
	handler := func(ctx context.Context, _ any) (any, error) {
		handlerContext = ctx
		return nil, nil
	}
	md := bearer(f.token(b, map[string]any{"shelf": "shelves/1", "email": "alice@example.com", "organization": "organizations/1"}))
	_, err := interceptor.Unary()(metadata.NewIncomingContext(context.Background(), md), nil, &grpc.UnaryServerInfo{FullMethod: echoMethod}, handler)
	require.NoError(b, err)
	ctx := credentials.NewContextWithRequestInfo(handlerContext, credentials.RequestInfo{Method: getBookMethod})
	credential := authentication.Credentials()
	tokenMetadata, err := credential.GetRequestMetadata(ctx)
	require.NoError(b, err)
	incoming := metadata.NewIncomingContext(context.Background(), metadata.New(tokenMetadata))
	info := &grpc.UnaryServerInfo{FullMethod: getBookMethod}
	request := &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"}
	unaryInterceptor := interceptor.Unary()

	b.Run("sign", func(b *testing.B) {
		for b.Loop() {
			if _, err := credential.GetRequestMetadata(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("verify and decide", func(b *testing.B) {
		for b.Loop() {
			if _, err := unaryInterceptor(incoming, request, info, handler); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sign, verify and decide", func(b *testing.B) {
		for b.Loop() {
			tokenMetadata, err := credential.GetRequestMetadata(ctx)
			if err != nil {
				b.Fatal(err)
			}
			incoming := metadata.NewIncomingContext(context.Background(), metadata.New(tokenMetadata))
			if _, err := unaryInterceptor(incoming, request, info, handler); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCall measures a whole call between two servers over a unix socket, with and without the server token.
func BenchmarkCall(b *testing.B) {
	p := newPlatform(b, newFixture(b))
	request := &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"}
	b.Run("server token", func(b *testing.B) {
		library := librarypb.NewLibraryServiceClient(p.dial(b, p.librarySocket, true))
		ctx := p.as(b, "worker")
		for b.Loop() {
			if _, err := library.GetBook(ctx, request); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("api key", func(b *testing.B) {
		library := librarypb.NewLibraryServiceClient(p.dial(b, p.librarySocket, false))
		ctx := metadata.NewOutgoingContext(context.Background(), apiKey("key-3"))
		for b.Loop() {
			if _, err := library.GetBook(ctx, request); err != nil {
				b.Fatal(err)
			}
		}
	})
}
