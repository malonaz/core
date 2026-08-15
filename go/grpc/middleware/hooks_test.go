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

func TestMustParseFullMethod(t *testing.T) {
	t.Run("splits a gRPC full method name on the last slash", func(t *testing.T) {
		service, method := MustParseFullMethod(testFullMethod)
		require.Equal(t, "malonaz.user.v1.UserService", service)
		require.Equal(t, "GetUser", method)
	})

	t.Run("panics without a leading slash", func(t *testing.T) {
		require.Panics(t, func() { MustParseFullMethod("malonaz.user.v1.UserService.GetUser") })
	})

	t.Run("panics when the method suffix is missing", func(t *testing.T) {
		require.Panics(t, func() { MustParseFullMethod("/malonaz.user.v1.UserService") })
	})
}

func TestMatchersPanicOnMalformedRegistration(t *testing.T) {
	// Caller-supplied names are a programming error: fail at registration.
	require.Panics(t, func() { MatchFullMethods("malonaz.user.v1.UserService.GetUser") })
	require.Panics(t, func() { MatchMethods("/malonaz.user.v1.UserService/GetUser") })
	require.Panics(t, func() { MatchServices("/malonaz.user.v1.UserService/GetUser") })
}

func TestMatchersRejectMalformedWireNames(t *testing.T) {
	// Names off the wire must not match, and must not panic: an
	// UnknownServiceHandler passes client-supplied strings straight through.
	require.NotPanics(t, func() {
		require.False(t, MatchMethods("GetUser")(context.Background(), "malformed"))
		require.False(t, MatchServices("malonaz.user.v1.UserService")(context.Background(), "malformed"))
		require.False(t, MatchFullMethods(testFullMethod)(context.Background(), "malformed"))
	})
}
