package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAppendToOutgoingContextWithPropagation(t *testing.T) {
	t.Run("unlimited propagation", func(t *testing.T) {
		ctx := AppendToOutgoingContextWithPropagation(context.Background(), "x-foo", "bar")
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"x-foo"}, md[propagateHeadersMetadataKey])
	})

	t.Run("with hops 1 does not add to propagate keys", func(t *testing.T) {
		ctx := AppendToOutgoingContextWithPropagation(context.Background(), "x-foo", "bar", WithHops(1))
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Empty(t, md[propagateHeadersMetadataKey])
	})

	t.Run("with hops 2 sets :1", func(t *testing.T) {
		ctx := AppendToOutgoingContextWithPropagation(context.Background(), "x-foo", "bar", WithHops(2))
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"x-foo:1"}, md[propagateHeadersMetadataKey])
	})

	t.Run("with hops 3 sets :2", func(t *testing.T) {
		ctx := AppendToOutgoingContextWithPropagation(context.Background(), "x-foo", "bar", WithHops(3))
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"x-foo:2"}, md[propagateHeadersMetadataKey])
	})
}

func TestPropagateHeadersToOutgoingContext(t *testing.T) {
	t.Run("unlimited propagation continues", func(t *testing.T) {
		incomingMD := metadata.Pairs("x-foo", "bar", propagateHeadersMetadataKey, "x-foo")
		ctx := metadata.NewIncomingContext(context.Background(), incomingMD)

		ctx = propagateHeadersToOutgoingContext(ctx)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"x-foo"}, md[propagateHeadersMetadataKey])
	})

	t.Run("hops 1 propagates but removes from propagate keys", func(t *testing.T) {
		incomingMD := metadata.Pairs("x-foo", "bar", propagateHeadersMetadataKey, "x-foo:1")
		ctx := metadata.NewIncomingContext(context.Background(), incomingMD)

		ctx = propagateHeadersToOutgoingContext(ctx)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Empty(t, md[propagateHeadersMetadataKey])
	})

	t.Run("hops 2 propagates and decrements to 1", func(t *testing.T) {
		incomingMD := metadata.Pairs("x-foo", "bar", propagateHeadersMetadataKey, "x-foo:2")
		ctx := metadata.NewIncomingContext(context.Background(), incomingMD)

		ctx = propagateHeadersToOutgoingContext(ctx)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"x-foo:1"}, md[propagateHeadersMetadataKey])
	})

	t.Run("no incoming metadata", func(t *testing.T) {
		ctx := propagateHeadersToOutgoingContext(context.Background())
		_, ok := metadata.FromOutgoingContext(ctx)
		require.False(t, ok)
	})

	t.Run("multiple headers mixed unlimited and last hop", func(t *testing.T) {
		incomingMD := metadata.Pairs(
			"x-foo", "bar",
			"x-baz", "qux",
			propagateHeadersMetadataKey, "x-foo",
			propagateHeadersMetadataKey, "x-baz:1",
		)
		ctx := metadata.NewIncomingContext(context.Background(), incomingMD)

		ctx = propagateHeadersToOutgoingContext(ctx)

		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"bar"}, md["x-foo"])
		require.Equal(t, []string{"qux"}, md["x-baz"])
		require.Equal(t, []string{"x-foo"}, md[propagateHeadersMetadataKey])
	})
}
