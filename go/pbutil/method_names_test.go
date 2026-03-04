package pbutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.TestService",
	Methods: []grpc.MethodDesc{
		{MethodName: "GetUser"},
		{MethodName: "ListUsers"},
		{MethodName: "DeleteUser"},
	},
}

func TestMustFullMethodNames(t *testing.T) {
	t.Run("single method", func(t *testing.T) {
		result := MustFullMethodNames(testServiceDesc, "GetUser")
		require.Equal(t, []string{"test.TestService.GetUser"}, result)
	})

	t.Run("multiple methods", func(t *testing.T) {
		result := MustFullMethodNames(testServiceDesc, "GetUser", "ListUsers")
		require.ElementsMatch(t, []string{"test.TestService.GetUser", "test.TestService.ListUsers"}, result)
	})

	t.Run("all methods", func(t *testing.T) {
		result := MustFullMethodNames(testServiceDesc, "GetUser", "ListUsers", "DeleteUser")
		require.ElementsMatch(t, []string{"test.TestService.GetUser", "test.TestService.ListUsers", "test.TestService.DeleteUser"}, result)
	})

	t.Run("panics on unknown method", func(t *testing.T) {
		require.Panics(t, func() {
			MustFullMethodNames(testServiceDesc, "NonExistent")
		})
	})

	t.Run("panics when one method is unknown", func(t *testing.T) {
		require.Panics(t, func() {
			MustFullMethodNames(testServiceDesc, "GetUser", "NonExistent")
		})
	})
}
