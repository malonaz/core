package require

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error is a convenience utility function to assert
// an error is a gRPC error matching the given gRPC code.
func Error(t *testing.T, code codes.Code, err error) {
	require.Error(t, err)
	status, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, status.Code())
}
