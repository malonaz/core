// go/test/user/user_service/sat/update_test.go
package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
	userpb "github.com/malonaz/core/genproto/test/user/v1"
)

func updateOrganization(t *testing.T, organization *userpb.Organization, paths []string) *userpb.Organization {
	t.Helper()
	updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
		Organization: organization,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedOrganization, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
	require.NoError(t, err)
	return updatedOrganization
}

func updateUser(t *testing.T, user *userpb.User, paths []string) *userpb.User {
	t.Helper()
	updateUserRequest := &userservicepb.UpdateUserRequest{
		User:       user,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}
	updatedUser, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
	require.NoError(t, err)
	return updatedUser
}

func TestUpdate_Organization(t *testing.T) {
	t.Parallel()

	t.Run("DisplayName", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Original")

		updated := updateOrganization(t, &userpb.Organization{
			Name:        original.Name,
			DisplayName: "Update Org Updated",
		}, []string{"display_name"})

		require.Equal(t, "Update Org Updated", updated.DisplayName)

		got := getOrganization(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("Labels", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Labels")

		updated := updateOrganization(t, &userpb.Organization{
			Name:   original.Name,
			Labels: map[string]string{"env": "prod", "tier": "premium"},
		}, []string{"labels"})

		require.Equal(t, "prod", updated.Labels["env"])
		require.Equal(t, "premium", updated.Labels["tier"])

		got := getOrganization(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("ClearLabels", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: &userpb.Organization{
				DisplayName: "Update Org Clear Labels",
				Labels:      map[string]string{"remove": "me"},
			},
		}
		original, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)

		updated := updateOrganization(t, &userpb.Organization{
			Name:   original.Name,
			Labels: map[string]string{},
		}, []string{"labels"})

		require.Empty(t, updated.Labels)

		got := getOrganization(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("UpdateTimeAdvances", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Time")

		updated := updateOrganization(t, &userpb.Organization{
			Name:        original.Name,
			DisplayName: "Time Check",
		}, []string{"display_name"})

		require.True(t,
			updated.UpdateTime.AsTime().After(original.UpdateTime.AsTime()) ||
				updated.UpdateTime.AsTime().Equal(original.UpdateTime.AsTime()))
	})

	t.Run("CreateTimeUnchanged", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org CreateTime")

		updated := updateOrganization(t, &userpb.Organization{
			Name:        original.Name,
			DisplayName: "CreateTime Check",
		}, []string{"display_name"})

		require.Equal(t, original.CreateTime.AsTime(), updated.CreateTime.AsTime())
	})

	t.Run("EtagChanges", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Etag")

		updated := updateOrganization(t, &userpb.Organization{
			Name:        original.Name,
			DisplayName: "Etag Check",
		}, []string{"display_name"})

		require.NotEqual(t, original.Etag, updated.Etag)
	})

	t.Run("PreservesUnmaskedFields", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: &userpb.Organization{
				DisplayName: "Update Org Preserve",
				Labels:      map[string]string{"keep": "this"},
			},
		}
		original, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)

		updated := updateOrganization(t, &userpb.Organization{
			Name:        original.Name,
			DisplayName: "Preserve Updated",
		}, []string{"display_name"})

		require.Equal(t, "this", updated.Labels["keep"])
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Unauth Name")
		updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
			Organization: &userpb.Organization{Name: original.Name},
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Unauth CT")
		updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
			Organization: &userpb.Organization{Name: original.Name},
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyMask", func(t *testing.T) {
		t.Parallel()
		original := createTestOrganization(t, "Update Org Empty Mask")
		updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
			Organization: &userpb.Organization{Name: original.Name},
			UpdateMask:   &fieldmaskpb.FieldMask{},
		}
		_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingName", func(t *testing.T) {
		t.Parallel()
		updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
			Organization: &userpb.Organization{DisplayName: "No Name"},
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		updateOrganizationRequest := &userservicepb.UpdateOrganizationRequest{
			Organization: &userpb.Organization{
				Name:        "organizations/nonexistent-update",
				DisplayName: "Ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := userServiceClient.UpdateOrganization(ctx, updateOrganizationRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestUpdate_User(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Update User Org")

	t.Run("SingleField_DisplayName", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Original")

		updated := updateUser(t, &userpb.User{
			Name:        original.Name,
			DisplayName: "Update User Updated",
		}, []string{"display_name"})

		require.Equal(t, "Update User Updated", updated.DisplayName)

		got := getUser(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("MultipleFields", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Multi")

		updated := updateUser(t, &userpb.User{
			Name:         original.Name,
			DisplayName:  "Multi Updated",
			EmailAddress: "multi-updated@example.com",
			PhoneNumber:  "+33612345678",
		}, []string{"display_name", "email_address", "phone_number"})

		require.Equal(t, "Multi Updated", updated.DisplayName)
		require.Equal(t, "multi-updated@example.com", updated.EmailAddress)
		require.Equal(t, "+33612345678", updated.PhoneNumber)

		got := getUser(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("Labels", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Labels")

		updated := updateUser(t, &userpb.User{
			Name:   original.Name,
			Labels: map[string]string{"env": "staging", "team": "backend"},
		}, []string{"labels"})

		require.Equal(t, "staging", updated.Labels["env"])
		require.Equal(t, "backend", updated.Labels["team"])

		got := getUser(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("MetadataFullReplacement", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Meta Full")

		updated := updateUser(t, &userpb.User{
			Name: original.Name,
			Metadata: &userpb.UserMetadata{
				PreferredLanguage: "fr",
				Timezone:          "Europe/Paris",
			},
		}, []string{"metadata"})

		require.Equal(t, "fr", updated.Metadata.PreferredLanguage)
		require.Equal(t, "Europe/Paris", updated.Metadata.Timezone)

		got := getUser(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("MetadataPartialUpdate", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Meta Partial")

		updated := updateUser(t, &userpb.User{
			Name: original.Name,
			Metadata: &userpb.UserMetadata{
				PreferredLanguage: "de",
			},
		}, []string{"metadata.preferred_language"})

		require.Equal(t, "de", updated.Metadata.PreferredLanguage)
		require.Equal(t, original.Metadata.Timezone, updated.Metadata.Timezone)

		got := getUser(t, original.Name)
		grpcrequire.Equal(t, updated, got)
	})

	t.Run("PreservesUnmaskedFields", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   validUser(),
		}
		original, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		updated := updateUser(t, &userpb.User{
			Name:        original.Name,
			DisplayName: "Preserve Check",
		}, []string{"display_name"})

		expected := proto.CloneOf(original)
		expected.DisplayName = "Preserve Check"
		grpcrequire.Equal(t, expected, updated,
			protocmp.IgnoreFields((*userpb.User)(nil), "update_time", "etag"))
	})

	t.Run("UpdateTimeAdvances", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Time")

		before := time.Now().UTC()
		updated := updateUser(t, &userpb.User{
			Name:        original.Name,
			DisplayName: "Time Advance",
		}, []string{"display_name"})
		after := time.Now().UTC()

		updateTime := updated.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before))
		require.True(t, !updateTime.After(after))
	})

	t.Run("EtagChanges", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Etag")

		updated := updateUser(t, &userpb.User{
			Name:        original.Name,
			DisplayName: "Etag Changed",
		}, []string{"display_name"})

		require.NotEqual(t, original.Etag, updated.Etag)
	})

	t.Run("ZeroValue_ClearString", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User ZeroStr")

		updated := updateUser(t, &userpb.User{
			Name:        original.Name,
			PhoneNumber: "",
		}, []string{"phone_number"})

		require.Empty(t, updated.PhoneNumber)

		got := getUser(t, original.Name)
		require.Empty(t, got.PhoneNumber)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Unauth Name")
		updateUserRequest := &userservicepb.UpdateUserRequest{
			User:       &userpb.User{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Unauth CT")
		updateUserRequest := &userservicepb.UpdateUserRequest{
			User:       &userpb.User{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyMask", func(t *testing.T) {
		t.Parallel()
		original := createTestUser(t, organization.Name, "Update User Empty Mask")
		updateUserRequest := &userservicepb.UpdateUserRequest{
			User:       &userpb.User{Name: original.Name},
			UpdateMask: &fieldmaskpb.FieldMask{},
		}
		_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingName", func(t *testing.T) {
		t.Parallel()
		updateUserRequest := &userservicepb.UpdateUserRequest{
			User:       &userpb.User{DisplayName: "No Name"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		updateUserRequest := &userservicepb.UpdateUserRequest{
			User: &userpb.User{
				Name:        organization.Name + "/users/nonexistent-update",
				DisplayName: "Ghost",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := userServiceClient.UpdateUser(ctx, updateUserRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}
