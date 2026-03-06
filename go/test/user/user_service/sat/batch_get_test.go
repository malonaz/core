// go/test/user/user_service/sat/batch_test.go
package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
)

func TestBatchGet_Organizations(t *testing.T) {
	t.Parallel()
	org1 := createTestOrganization(t, "Batch Org 1")
	org2 := createTestOrganization(t, "Batch Org 2")
	org3 := createTestOrganization(t, "Batch Org 3")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org1.Name, org2.Name, org3.Name},
		}
		batchGetOrganizationsResponse, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetOrganizationsResponse.Organizations, 3)
	})

	t.Run("SingleResource", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org1.Name},
		}
		batchGetOrganizationsResponse, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetOrganizationsResponse.Organizations, 1)
		require.Equal(t, org1.Name, batchGetOrganizationsResponse.Organizations[0].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org3.Name, org1.Name, org2.Name},
		}
		batchGetOrganizationsResponse, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetOrganizationsResponse.Organizations, 3)
		require.Equal(t, org3.Name, batchGetOrganizationsResponse.Organizations[0].Name)
		require.Equal(t, org1.Name, batchGetOrganizationsResponse.Organizations[1].Name)
		require.Equal(t, org2.Name, batchGetOrganizationsResponse.Organizations[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org1.Name, org2.Name},
		}
		batchGetOrganizationsResponse, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetOrganizationsResponse.Organizations, 2)

		gotOrg1 := getOrganization(t, org1.Name)
		gotOrg2 := getOrganization(t, org2.Name)
		grpcrequire.Equal(t, gotOrg1, batchGetOrganizationsResponse.Organizations[0])
		grpcrequire.Equal(t, gotOrg2, batchGetOrganizationsResponse.Organizations[1])
	})

	t.Run("SoftDeletedReturned", func(t *testing.T) {
		t.Parallel()
		deletedOrg := createTestOrganization(t, "Batch Deleted Org")
		deleteOrganizationRequest := &userservicepb.DeleteOrganizationRequest{Name: deletedOrg.Name}
		_, err := userServiceClient.DeleteOrganization(ctx, deleteOrganizationRequest)
		require.NoError(t, err)

		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{deletedOrg.Name},
		}
		batchGetOrganizationsResponse, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetOrganizationsResponse.Organizations, 1)
		require.NotNil(t, batchGetOrganizationsResponse.Organizations[0].DeleteTime)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org1.Name, "organizations/nonexistent-batch"},
		}
		_, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{},
		}
		_, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{org1.Name, org1.Name},
		}
		_, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyNameEntry", func(t *testing.T) {
		t.Parallel()
		batchGetOrganizationsRequest := &userservicepb.BatchGetOrganizationsRequest{
			Names: []string{""},
		}
		_, err := userServiceClient.BatchGetOrganizations(ctx, batchGetOrganizationsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestBatchGet_Users(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Batch User Org")
	user1 := createTestUser(t, organization.Name, "Batch User 1")
	user2 := createTestUser(t, organization.Name, "Batch User 2")
	user3 := createTestUser(t, organization.Name, "Batch User 3")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user1.Name, user2.Name, user3.Name},
		}
		batchGetUsersResponse, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		require.NoError(t, err)
		require.Len(t, batchGetUsersResponse.Users, 3)
	})

	t.Run("SingleResource", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user1.Name},
		}
		batchGetUsersResponse, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		require.NoError(t, err)
		require.Len(t, batchGetUsersResponse.Users, 1)
		require.Equal(t, user1.Name, batchGetUsersResponse.Users[0].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user3.Name, user1.Name, user2.Name},
		}
		batchGetUsersResponse, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		require.NoError(t, err)
		require.Len(t, batchGetUsersResponse.Users, 3)
		require.Equal(t, user3.Name, batchGetUsersResponse.Users[0].Name)
		require.Equal(t, user1.Name, batchGetUsersResponse.Users[1].Name)
		require.Equal(t, user2.Name, batchGetUsersResponse.Users[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user1.Name, user2.Name},
		}
		batchGetUsersResponse, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		require.NoError(t, err)
		require.Len(t, batchGetUsersResponse.Users, 2)

		gotUser1 := getUser(t, user1.Name)
		gotUser2 := getUser(t, user2.Name)
		grpcrequire.Equal(t, gotUser1, batchGetUsersResponse.Users[0])
		grpcrequire.Equal(t, gotUser2, batchGetUsersResponse.Users[1])
	})

	t.Run("SoftDeletedReturned", func(t *testing.T) {
		t.Parallel()
		deletedUser := createTestUser(t, organization.Name, "Batch Deleted User")
		deleteUserRequest := &userservicepb.DeleteUserRequest{Name: deletedUser.Name}
		_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
		require.NoError(t, err)

		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{deletedUser.Name},
		}
		batchGetUsersResponse, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		require.NoError(t, err)
		require.Len(t, batchGetUsersResponse.Users, 1)
		require.NotNil(t, batchGetUsersResponse.Users[0].DeleteTime)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user1.Name, organization.Name + "/users/nonexistent-batch"},
		}
		_, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{},
		}
		_, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{user1.Name, user1.Name},
		}
		_, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyNameEntry", func(t *testing.T) {
		t.Parallel()
		batchGetUsersRequest := &userservicepb.BatchGetUsersRequest{
			Parent: organization.Name,
			Names:  []string{""},
		}
		_, err := userServiceClient.BatchGetUsers(ctx, batchGetUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
