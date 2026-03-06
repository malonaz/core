// go/test/user/user_service/sat/get_test.go
package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
	userpb "github.com/malonaz/core/genproto/test/user/v1"
)

func getOrganization(t *testing.T, name string) *userpb.Organization {
	t.Helper()
	getOrganizationRequest := &userservicepb.GetOrganizationRequest{Name: name}
	organization, err := userServiceClient.GetOrganization(ctx, getOrganizationRequest)
	require.NoError(t, err)
	return organization
}

func getUser(t *testing.T, name string) *userpb.User {
	t.Helper()
	getUserRequest := &userservicepb.GetUserRequest{Name: name}
	user, err := userServiceClient.GetUser(ctx, getUserRequest)
	require.NoError(t, err)
	return user
}

func TestGet_Organization(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Get Organization")
		got := getOrganization(t, organization.Name)
		require.Equal(t, organization.Name, got.Name)
		require.Equal(t, "Get Organization", got.DisplayName)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		getOrganizationRequest := &userservicepb.GetOrganizationRequest{
			Name: "organizations/nonexistent-org",
		}
		_, err := userServiceClient.GetOrganization(ctx, getOrganizationRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		t.Parallel()
		getOrganizationRequest := &userservicepb.GetOrganizationRequest{}
		_, err := userServiceClient.GetOrganization(ctx, getOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestGet_User(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Get User Org")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Get User")
		got := getUser(t, user.Name)
		require.Equal(t, user.Name, got.Name)
		require.Equal(t, "Get User", got.DisplayName)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		getUserRequest := &userservicepb.GetUserRequest{
			Name: organization.Name + "/users/nonexistent-user",
		}
		_, err := userServiceClient.GetUser(ctx, getUserRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		t.Parallel()
		getUserRequest := &userservicepb.GetUserRequest{}
		_, err := userServiceClient.GetUser(ctx, getUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
