// go/test/user/user_service/sat/delete_test.go
package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
)

func TestDelete_Organization(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsDeleteTime", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Time")

		before := time.Now().UTC()
		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{
			Name: organization.Name,
		}
		deletedOrganization, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotNil(t, deletedOrganization.DeleteTime)
		deleteTime := deletedOrganization.DeleteTime.AsTime()
		require.True(t, !deleteTime.Before(before))
		require.True(t, !deleteTime.After(after))
	})

	t.Run("GetReturnsDeletedResource", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Get")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		got := getOrganization(t, organization.Name)
		require.NotNil(t, got.DeleteTime)
		require.Equal(t, organization.DisplayName, got.DisplayName)
	})

	t.Run("HiddenFromList", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Hidden 11223")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			Filter: `display_name = "Delete Org Hidden 11223"`,
		}
		listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
		require.Empty(t, listOrganizationsResponse.Organizations)
	})

	t.Run("ShowDeletedReveals", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org ShowDel 22334")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			Filter:      `display_name = "Delete Org ShowDel 22334"`,
			ShowDeleted: true,
		}
		listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, listOrganizationsResponse.Organizations, 1)
		require.NotNil(t, listOrganizationsResponse.Organizations[0].DeleteTime)
	})

	t.Run("DeleteAlreadyDeletedReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Twice")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		_, err = userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("AllowMissing_AlreadyDeleted", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org AllowMissing")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		deleteOrganizationRequest = &userservicepb.DeleteOrganizationRequest{
			Name:         organization.Name,
			AllowMissing: true,
		}
		_, err = userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_NeverExisted", func(t *testing.T) {
		t.Parallel()
		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{
			Name:         "organizations/never-existed-org",
			AllowMissing: true,
		}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{
			Name: "organizations/nonexistent-org-del",
		}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Etag Match")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{
			Name: organization.Name,
			Etag: organization.Etag,
		}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)
	})

	t.Run("EtagMismatch", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Etag Bad")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{
			Name: organization.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("PreservesFields", func(t *testing.T) {
		t.Parallel()
		organization := createTestOrganization(t, "Delete Org Preserve")

		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: organization.Name}
		deletedOrganization, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		require.Equal(t, organization.Name, deletedOrganization.Name)
		require.Equal(t, organization.DisplayName, deletedOrganization.DisplayName)
		require.Equal(t, organization.CreateTime.AsTime(), deletedOrganization.CreateTime.AsTime())
	})
}

func TestDelete_User(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Delete User Org")

	t.Run("ReturnsDeleteTime", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Time")

		before := time.Now().UTC()
		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		deletedUser, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotNil(t, deletedUser.DeleteTime)
		deleteTime := deletedUser.DeleteTime.AsTime()
		require.True(t, !deleteTime.Before(before))
		require.True(t, !deleteTime.After(after))
	})

	t.Run("GetReturnsDeletedResource", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Get")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		got := getUser(t, user.Name)
		require.NotNil(t, got.DeleteTime)
		require.Equal(t, user.DisplayName, got.DisplayName)
	})

	t.Run("HiddenFromList", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Hidden 33445")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `display_name = "Delete User Hidden 33445"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Empty(t, listUsersResponse.Users)
	})

	t.Run("ShowDeletedReveals", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User ShowDel 44556")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent:      organization.Name,
			Filter:      `display_name = "Delete User ShowDel 44556"`,
			ShowDeleted: true,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
		require.NotNil(t, listUsersResponse.Users[0].DeleteTime)
	})

	t.Run("DeleteAlreadyDeletedReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Twice")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		_, err = userServiceClient.DeleteUser(ctx, deleteUserRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("AllowMissing_AlreadyDeleted", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User AllowMissing")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		deleteUserRequest = &userservicepb.DeleteUserRequest{
			Name:         user.Name,
			AllowMissing: true,
		}
		_, err = userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_NeverExisted", func(t *testing.T) {
		t.Parallel()
		deleteUserRequest := &userservicepb.DeleteUserRequest{
			Name:         organization.Name + "/users/never-existed-user",
			AllowMissing: true,
		}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteUserRequest := &userservicepb.DeleteUserRequest{
			Name: organization.Name + "/users/nonexistent-user-del",
		}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Etag Match")

		deleteUserRequest := &userservicepb.DeleteUserRequest{
			Name: user.Name,
			Etag: user.Etag,
		}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)
	})

	t.Run("EtagMismatch", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Etag Bad")

		deleteUserRequest := &userservicepb.DeleteUserRequest{
			Name: user.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("PreservesFields", func(t *testing.T) {
		t.Parallel()
		user := createTestUser(t, organization.Name, "Delete User Preserve")

		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
		deletedUser, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		require.Equal(t, user.Name, deletedUser.Name)
		require.Equal(t, user.DisplayName, deletedUser.DisplayName)
		require.Equal(t, user.EmailAddress, deletedUser.EmailAddress)
		require.Equal(t, user.CreateTime.AsTime(), deletedUser.CreateTime.AsTime())
	})
}
