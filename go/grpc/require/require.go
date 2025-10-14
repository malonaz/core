package require

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// Error is a convenience utility function to assert
// an error is a gRPC error matching the given gRPC code.
func Error(t *testing.T, code codes.Code, err error) {
	require.Error(t, err)
	status, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, status.Code())
}

func Equal(t *testing.T, expected, actual proto.Message, opts ...cmp.Option) {
	allOpts := append([]cmp.Option{protocmp.Transform()}, opts...)
	diff := cmp.Diff(expected, actual, allOpts...)
	require.Empty(t, diff, diff)
}

func NotEqual(t *testing.T, expected, actual proto.Message, opts ...cmp.Option) {
	allOpts := append([]cmp.Option{protocmp.Transform()}, opts...)
	diff := cmp.Diff(expected, actual, allOpts...)
	require.NotEmpty(t, diff, diff)
}
