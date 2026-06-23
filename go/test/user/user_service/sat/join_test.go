package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
	userpb "github.com/malonaz/core/genproto/test/user/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func getUserProfile(t *testing.T, name string) *userpb.UserProfile {
	t.Helper()
	getUserProfileRequest := &userservicepb.GetUserProfileRequest{Name: name}
	userProfile, err := userServiceClient.GetUserProfile(ctx, getUserProfileRequest)
	require.NoError(t, err)
	return userProfile
}

// ---------- Auto-creation (Create User → Profile join fields) ----------

func TestUserProfile_JoinFieldsPopulated_OnAutoCreation(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join AutoCreate Org")
	user := createTestUser(t, organization.Name, "Join AutoCreate User")

	profile := getUserProfile(t, user.Name+"/profile")
	require.Equal(t, "Join AutoCreate User", profile.UserDisplayName)
	require.Equal(t, user.EmailAddress, profile.UserEmailAddress)
	require.Equal(t, "Join AutoCreate Org", profile.OrganizationDisplayName)
}

func TestUserProfile_JoinFieldsPopulated_OnAutoCreation_IdempotentCreate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join Idempotent Org")

	createUserRequest := &userservicepb.CreateUserRequest{
		Parent: organization.Name,
		User: &userpb.User{
			DisplayName:  "Join Idempotent User",
			EmailAddress: "join-idempotent@example.com",
			Metadata:     &userpb.UserMetadata{},
		},
		RequestId: "b2c3d4e5-f6a7-8901-bcde-f12345670002",
	}
	user1, err := userServiceClient.CreateUser(ctx, createUserRequest)
	require.NoError(t, err)

	user2, err := userServiceClient.CreateUser(ctx, createUserRequest)
	require.NoError(t, err)
	require.Equal(t, user1.Name, user2.Name)

	profile := getUserProfile(t, user1.Name+"/profile")
	require.Equal(t, "Join Idempotent User", profile.UserDisplayName)
	require.Equal(t, "join-idempotent@example.com", profile.UserEmailAddress)
	require.Equal(t, "Join Idempotent Org", profile.OrganizationDisplayName)
}

func TestUserProfile_JoinFieldsPopulated_MultipleUsersUnderSameOrg(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join MultiUser Org")
	userA := createTestUser(t, organization.Name, "Join MultiUser A")
	userB := createTestUser(t, organization.Name, "Join MultiUser B")

	profileA := getUserProfile(t, userA.Name+"/profile")
	profileB := getUserProfile(t, userB.Name+"/profile")

	require.Equal(t, "Join MultiUser A", profileA.UserDisplayName)
	require.Equal(t, userA.EmailAddress, profileA.UserEmailAddress)
	require.Equal(t, "Join MultiUser Org", profileA.OrganizationDisplayName)

	require.Equal(t, "Join MultiUser B", profileB.UserDisplayName)
	require.Equal(t, userB.EmailAddress, profileB.UserEmailAddress)
	require.Equal(t, "Join MultiUser Org", profileB.OrganizationDisplayName)
}

// ---------- Get ----------

func TestUserProfile_JoinFields_Get(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join Get Org")
	user := createTestUser(t, organization.Name, "Join Get User")

	profile := getUserProfile(t, user.Name+"/profile")
	require.Equal(t, "Join Get User", profile.UserDisplayName)
	require.Equal(t, user.EmailAddress, profile.UserEmailAddress)
	require.Equal(t, "Join Get Org", profile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_Get_AfterParentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join GetPU Org")
	user := createTestUser(t, organization.Name, "Original GetPU User")

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:         user.Name,
			DisplayName:  "Updated GetPU User",
			EmailAddress: "updated-getpu@example.com",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "email_address"}},
	}
	_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	profile := getUserProfile(t, user.Name+"/profile")
	require.Equal(t, "Updated GetPU User", profile.UserDisplayName)
	require.Equal(t, "updated-getpu@example.com", profile.UserEmailAddress)
	require.Equal(t, "Join GetPU Org", profile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_Get_AfterGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original GetGPU Org")
	user := createTestUser(t, organization.Name, "Join GetGPU User")

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated GetGPU Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	profile := getUserProfile(t, user.Name+"/profile")
	require.Equal(t, "Updated GetGPU Org", profile.OrganizationDisplayName)
	require.Equal(t, "Join GetGPU User", profile.UserDisplayName)
}

func TestUserProfile_JoinFields_Get_AfterBothParentAndGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original Both Org")
	user := createTestUser(t, organization.Name, "Original Both User")

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated Both Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:         user.Name,
			DisplayName:  "Updated Both User",
			EmailAddress: "updated-both@example.com",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "email_address"}},
	}
	_, err = userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	profile := getUserProfile(t, user.Name+"/profile")
	require.Equal(t, "Updated Both User", profile.UserDisplayName)
	require.Equal(t, "updated-both@example.com", profile.UserEmailAddress)
	require.Equal(t, "Updated Both Org", profile.OrganizationDisplayName)
}

// ---------- UpdateUserProfile response ----------

func TestUserProfile_JoinFields_UpdateResponse(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join UpdResp Org")
	user := createTestUser(t, organization.Name, "Join UpdResp User")
	profileName := user.Name + "/profile"

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "some bio",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	updatedProfile, err := userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	require.NoError(t, err)
	require.Equal(t, "some bio", updatedProfile.Bio)
	require.Equal(t, "Join UpdResp User", updatedProfile.UserDisplayName)
	require.Equal(t, user.EmailAddress, updatedProfile.UserEmailAddress)
	require.Equal(t, "Join UpdResp Org", updatedProfile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_UpdateResponse_AfterParentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join UpdRespPU Org")
	user := createTestUser(t, organization.Name, "Original UpdRespPU User")
	profileName := user.Name + "/profile"

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:         user.Name,
			DisplayName:  "Updated UpdRespPU User",
			EmailAddress: "updated-updresppu@example.com",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "email_address"}},
	}
	_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "bio after parent update",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	updatedProfile, err := userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	require.NoError(t, err)
	require.Equal(t, "Updated UpdRespPU User", updatedProfile.UserDisplayName)
	require.Equal(t, "updated-updresppu@example.com", updatedProfile.UserEmailAddress)
	require.Equal(t, "Join UpdRespPU Org", updatedProfile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_UpdateResponse_AfterGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original UpdRespGPU Org")
	user := createTestUser(t, organization.Name, "Join UpdRespGPU User")
	profileName := user.Name + "/profile"

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated UpdRespGPU Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "bio after grandparent update",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	updatedProfile, err := userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	require.NoError(t, err)
	require.Equal(t, "Join UpdRespGPU User", updatedProfile.UserDisplayName)
	require.Equal(t, "Updated UpdRespGPU Org", updatedProfile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_UpdateResponse_AfterBothParentAndGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original UpdRespBoth Org")
	user := createTestUser(t, organization.Name, "Original UpdRespBoth User")
	profileName := user.Name + "/profile"

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated UpdRespBoth Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:        user.Name,
			DisplayName: "Updated UpdRespBoth User",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err = userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "bio after both updates",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	updatedProfile, err := userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	require.NoError(t, err)
	require.Equal(t, "Updated UpdRespBoth User", updatedProfile.UserDisplayName)
	require.Equal(t, "Updated UpdRespBoth Org", updatedProfile.OrganizationDisplayName)
}

// ---------- List ----------

func TestUserProfile_JoinFields_List(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join List Org")
	user := createTestUser(t, organization.Name, "Join List User")

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent: user.Name,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)
	require.Equal(t, "Join List User", listUserProfilesResponse.UserProfiles[0].UserDisplayName)
	require.Equal(t, user.EmailAddress, listUserProfilesResponse.UserProfiles[0].UserEmailAddress)
	require.Equal(t, "Join List Org", listUserProfilesResponse.UserProfiles[0].OrganizationDisplayName)
}

func TestUserProfile_JoinFields_List_AfterParentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ListPU Org")
	user := createTestUser(t, organization.Name, "Original ListPU User")

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:        user.Name,
			DisplayName: "Updated ListPU User",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent: user.Name,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)
	require.Equal(t, "Updated ListPU User", listUserProfilesResponse.UserProfiles[0].UserDisplayName)
	require.Equal(t, "Join ListPU Org", listUserProfilesResponse.UserProfiles[0].OrganizationDisplayName)
}

func TestUserProfile_JoinFields_List_AfterGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original ListGPU Org")
	user := createTestUser(t, organization.Name, "Join ListGPU User")

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated ListGPU Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent: user.Name,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)
	require.Equal(t, "Updated ListGPU Org", listUserProfilesResponse.UserProfiles[0].OrganizationDisplayName)
	require.Equal(t, "Join ListGPU User", listUserProfilesResponse.UserProfiles[0].UserDisplayName)
}

func TestUserProfile_JoinFields_List_ShowDeleted_AfterParentDeletion(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ListDel Org")
	user := createTestUser(t, organization.Name, "Join ListDel User")

	deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
	_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
	require.NoError(t, err)

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent:      user.Name,
		ShowDeleted: true,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)
	require.NotNil(t, listUserProfilesResponse.UserProfiles[0].DeleteTime)
	require.Equal(t, "Join ListDel User", listUserProfilesResponse.UserProfiles[0].UserDisplayName)
	require.Equal(t, "Join ListDel Org", listUserProfilesResponse.UserProfiles[0].OrganizationDisplayName)
}

// ---------- BatchGet ----------

func TestUserProfile_JoinFields_BatchGet(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join BatchGet Org")
	userA := createTestUser(t, organization.Name, "Join BatchGet User A")
	userB := createTestUser(t, organization.Name, "Join BatchGet User B")
	parent := organization.Name + "/users/-"
	profileNameA := userA.Name + "/profile"
	profileNameB := userB.Name + "/profile"

	batchGetUserProfilesRequest := &userservicepb.BatchGetUserProfilesRequest{
		Parent: parent,
		Names:  []string{profileNameA, profileNameB},
	}
	batchGetUserProfilesResponse, err := userServiceClient.BatchGetUserProfiles(ctx, batchGetUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetUserProfilesResponse.UserProfiles, 2)

	profileNameToUserProfile := map[string]*userpb.UserProfile{}
	for _, profile := range batchGetUserProfilesResponse.UserProfiles {
		profileNameToUserProfile[profile.Name] = profile
	}
	require.Equal(t, "Join BatchGet User A", profileNameToUserProfile[profileNameA].UserDisplayName)
	require.Equal(t, userA.EmailAddress, profileNameToUserProfile[profileNameA].UserEmailAddress)
	require.Equal(t, "Join BatchGet Org", profileNameToUserProfile[profileNameA].OrganizationDisplayName)
	require.Equal(t, "Join BatchGet User B", profileNameToUserProfile[profileNameB].UserDisplayName)
	require.Equal(t, userB.EmailAddress, profileNameToUserProfile[profileNameB].UserEmailAddress)
	require.Equal(t, "Join BatchGet Org", profileNameToUserProfile[profileNameB].OrganizationDisplayName)
}

func TestUserProfile_JoinFields_BatchGet_AfterParentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join BatchGetPU Org")
	userA := createTestUser(t, organization.Name, "Original BatchGetPU A")
	userB := createTestUser(t, organization.Name, "Join BatchGetPU B")
	parent := organization.Name + "/users/-"
	profileNameA := userA.Name + "/profile"
	profileNameB := userB.Name + "/profile"

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:        userA.Name,
			DisplayName: "Updated BatchGetPU A",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	batchGetUserProfilesRequest := &userservicepb.BatchGetUserProfilesRequest{
		Parent: parent,
		Names:  []string{profileNameA, profileNameB},
	}
	batchGetUserProfilesResponse, err := userServiceClient.BatchGetUserProfiles(ctx, batchGetUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetUserProfilesResponse.UserProfiles, 2)

	profileNameToUserProfile := map[string]*userpb.UserProfile{}
	for _, profile := range batchGetUserProfilesResponse.UserProfiles {
		profileNameToUserProfile[profile.Name] = profile
	}
	require.Equal(t, "Updated BatchGetPU A", profileNameToUserProfile[profileNameA].UserDisplayName)
	require.Equal(t, "Join BatchGetPU B", profileNameToUserProfile[profileNameB].UserDisplayName)
}

func TestUserProfile_JoinFields_BatchGet_AfterGrandparentUpdate(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original BatchGetGPU Org")
	userA := createTestUser(t, organization.Name, "Join BatchGetGPU A")
	userB := createTestUser(t, organization.Name, "Join BatchGetGPU B")
	parent := organization.Name + "/users/-"
	profileNameA := userA.Name + "/profile"
	profileNameB := userB.Name + "/profile"

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated BatchGetGPU Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	batchGetUserProfilesRequest := &userservicepb.BatchGetUserProfilesRequest{
		Parent: parent,
		Names:  []string{profileNameA, profileNameB},
	}
	batchGetUserProfilesResponse, err := userServiceClient.BatchGetUserProfiles(ctx, batchGetUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetUserProfilesResponse.UserProfiles, 2)

	for _, profile := range batchGetUserProfilesResponse.UserProfiles {
		require.Equal(t, "Updated BatchGetGPU Org", profile.OrganizationDisplayName)
	}
}

// ---------- Delete (soft delete cascading from parent/grandparent) ----------

func TestUserProfile_JoinFields_AfterParentDeletion(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ParentDel Org")
	user := createTestUser(t, organization.Name, "Join ParentDel User")
	profileName := user.Name + "/profile"

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "bio before delete",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err := userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	require.NoError(t, err)

	deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
	_, err = userServiceClient.DeleteUser(ctx, deleteUserRequest)
	require.NoError(t, err)

	profile, err := userServiceClient.GetUserProfile(ctx, &userservicepb.GetUserProfileRequest{Name: profileName})
	require.NoError(t, err)
	require.NotNil(t, profile.DeleteTime)
	require.Equal(t, "bio before delete", profile.Bio)
	require.Equal(t, "Join ParentDel User", profile.UserDisplayName)
	require.Equal(t, user.EmailAddress, profile.UserEmailAddress)
	require.Equal(t, "Join ParentDel Org", profile.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_AfterParentDeletion_UpdateFails(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ParentDelUpd Org")
	user := createTestUser(t, organization.Name, "Join ParentDelUpd User")
	profileName := user.Name + "/profile"

	deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
	_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
	require.NoError(t, err)

	updateUserProfileRequest := &userservicepb.UpdateUserProfileRequest{
		UserProfile: &userpb.UserProfile{
			Name: profileName,
			Bio:  "should fail",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bio"}},
	}
	_, err = userServiceClient.UpdateUserProfile(ctx, updateUserProfileRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestUserProfile_JoinFields_AfterParentDeletion_HiddenFromList(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ParentDelList Org")
	user := createTestUser(t, organization.Name, "Join ParentDelList User")

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent: user.Name,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)

	deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
	_, err = userServiceClient.DeleteUser(ctx, deleteUserRequest)
	require.NoError(t, err)

	listUserProfilesResponse, err = userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Empty(t, listUserProfilesResponse.UserProfiles)
}

func TestUserProfile_JoinFields_AfterParentDeletion_ShowDeletedReveals(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Join ParentDelShow Org")
	user := createTestUser(t, organization.Name, "Join ParentDelShow User")

	deleteUserRequest := &userservicepb.DeleteUserRequest{Name: user.Name}
	_, err := userServiceClient.DeleteUser(ctx, deleteUserRequest)
	require.NoError(t, err)

	listUserProfilesRequest := &userservicepb.ListUserProfilesRequest{
		Parent:      user.Name,
		ShowDeleted: true,
	}
	listUserProfilesResponse, err := userServiceClient.ListUserProfiles(ctx, listUserProfilesRequest)
	require.NoError(t, err)
	require.Len(t, listUserProfilesResponse.UserProfiles, 1)
	require.NotNil(t, listUserProfilesResponse.UserProfiles[0].DeleteTime)
	require.Equal(t, "Join ParentDelShow User", listUserProfilesResponse.UserProfiles[0].UserDisplayName)
	require.Equal(t, "Join ParentDelShow Org", listUserProfilesResponse.UserProfiles[0].OrganizationDisplayName)
}

// ---------- Grandparent join independence ----------

func TestUserProfile_GrandparentJoin_UpdateReflectsAcrossAllUsersProfiles(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Original GrandInd Org")
	userA := createTestUser(t, organization.Name, "GrandInd User A")
	userB := createTestUser(t, organization.Name, "GrandInd User B")
	userC := createTestUser(t, organization.Name, "GrandInd User C")

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        organization.Name,
			DisplayName: "Updated GrandInd Org",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	for _, user := range []*userpb.User{userA, userB, userC} {
		profile := getUserProfile(t, user.Name+"/profile")
		require.Equal(t, "Updated GrandInd Org", profile.OrganizationDisplayName)
	}

	require.Equal(t, "GrandInd User A", getUserProfile(t, userA.Name+"/profile").UserDisplayName)
	require.Equal(t, "GrandInd User B", getUserProfile(t, userB.Name+"/profile").UserDisplayName)
	require.Equal(t, "GrandInd User C", getUserProfile(t, userC.Name+"/profile").UserDisplayName)
}

func TestUserProfile_ParentJoin_UpdateOnlyAffectsOwnProfile(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "ParentInd Org")
	userA := createTestUser(t, organization.Name, "ParentInd User A")
	userB := createTestUser(t, organization.Name, "ParentInd User B")

	updateUserRequest := &userservicepb.UpdateUserRequest{
		User: &userpb.User{
			Name:        userA.Name,
			DisplayName: "Updated ParentInd A",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)

	profileA := getUserProfile(t, userA.Name+"/profile")
	profileB := getUserProfile(t, userB.Name+"/profile")
	require.Equal(t, "Updated ParentInd A", profileA.UserDisplayName)
	require.Equal(t, "ParentInd User B", profileB.UserDisplayName)
	require.Equal(t, "ParentInd Org", profileA.OrganizationDisplayName)
	require.Equal(t, "ParentInd Org", profileB.OrganizationDisplayName)
}

func TestUserProfile_JoinFields_DifferentOrgsIndependent(t *testing.T) {
	t.Parallel()
	orgA := createTestOrganization(t, "Org A Independent")
	orgB := createTestOrganization(t, "Org B Independent")
	userA := createTestUser(t, orgA.Name, "User Under Org A")
	userB := createTestUser(t, orgB.Name, "User Under Org B")

	profileA := getUserProfile(t, userA.Name+"/profile")
	profileB := getUserProfile(t, userB.Name+"/profile")
	require.Equal(t, "Org A Independent", profileA.OrganizationDisplayName)
	require.Equal(t, "Org B Independent", profileB.OrganizationDisplayName)

	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: &userpb.Organization{
			Name:        orgA.Name,
			DisplayName: "Org A Updated",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)

	profileA = getUserProfile(t, userA.Name+"/profile")
	profileB = getUserProfile(t, userB.Name+"/profile")
	require.Equal(t, "Org A Updated", profileA.OrganizationDisplayName)
	require.Equal(t, "Org B Independent", profileB.OrganizationDisplayName)
}
