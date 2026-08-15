package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Matchers receive gRPC's info.FullMethod verbatim, which is exactly the value
// of the generated *_FullMethodName constants.
const testFullMethod = "/malonaz.user.v1.UserService/GetUser"

func TestMatchFullMethods(t *testing.T) {
	t.Run("matches the generated full method name", func(t *testing.T) {
		matcher := MatchFullMethods(testFullMethod)
		require.True(t, matcher(context.Background(), testFullMethod))
	})

	t.Run("does not match a different method", func(t *testing.T) {
		matcher := MatchFullMethods("/malonaz.user.v1.UserService/ListUsers")
		require.False(t, matcher(context.Background(), testFullMethod))
	})
}

func TestMatchMethods(t *testing.T) {
	t.Run("matches on the bare method name", func(t *testing.T) {
		matcher := MatchMethods("GetUser")
		require.True(t, matcher(context.Background(), testFullMethod))
	})

	t.Run("does not match a different method name", func(t *testing.T) {
		matcher := MatchMethods("ListUsers")
		require.False(t, matcher(context.Background(), testFullMethod))
	})

	t.Run("matches the same method on any service", func(t *testing.T) {
		matcher := MatchMethods("GetUser")
		require.True(t, matcher(context.Background(), "/other.v1.OtherService/GetUser"))
	})
}

func TestMatchServices(t *testing.T) {
	t.Run("matches on the service full name", func(t *testing.T) {
		matcher := MatchServices("malonaz.user.v1.UserService")
		require.True(t, matcher(context.Background(), testFullMethod))
	})

	t.Run("does not match a different service", func(t *testing.T) {
		matcher := MatchServices("malonaz.user.v1.OtherService")
		require.False(t, matcher(context.Background(), testFullMethod))
	})
}

func TestServiceAndMethodNameOf(t *testing.T) {
	t.Run("splits a gRPC full method name", func(t *testing.T) {
		require.Equal(t, "malonaz.user.v1.UserService", serviceNameOf(testFullMethod))
		require.Equal(t, "GetUser", methodNameOf(testFullMethod))
	})

	t.Run("returns empty for a malformed name", func(t *testing.T) {
		require.Empty(t, serviceNameOf("malformed"))
		require.Empty(t, methodNameOf("malformed"))
	})
}
