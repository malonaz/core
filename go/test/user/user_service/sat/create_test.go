// go/test/user/user_service/sat/create_test.go
package sat

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
	userpb "github.com/malonaz/core/genproto/test/user/v1"
)

func validOrganization() *userpb.Organization {
	return &userpb.Organization{
		DisplayName: "Test Organization",
		Labels:      map[string]string{"env": "test"},
	}
}

func validUser() *userpb.User {
	return &userpb.User{
		DisplayName:  "Jane Doe",
		EmailAddress: "jane@example.com",
		PhoneNumber:  "+14155551234",
		Labels:       map[string]string{"role": "admin"},
		Metadata: &userpb.UserMetadata{
			PreferredLanguage: "en",
			Timezone:          "America/New_York",
		},
	}
}

func createTestOrganization(t *testing.T, displayName string) *userpb.Organization {
	t.Helper()
	createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
		Organization: &userpb.Organization{
			DisplayName: displayName,
		},
	}
	organization, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
	require.NoError(t, err)
	return organization
}

func createTestUser(t *testing.T, parent, displayName string) *userpb.User {
	t.Helper()
	createUserRequest := &userservicepb.CreateUserRequest{
		Parent: parent,
		User: &userpb.User{
			DisplayName:  displayName,
			EmailAddress: "test@example.com",
			PhoneNumber:  "+12025551234",
			Metadata: &userpb.UserMetadata{
				PreferredLanguage: "en",
				Timezone:          "UTC",
			},
		},
	}
	user, err := userServiceClient.CreateUser(ctx, createUserRequest)
	require.NoError(t, err)
	return user
}

func TestCreate_Organization(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: validOrganization(),
		}
		createdOrganization, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotEmpty(t, createdOrganization.Name)
		require.True(t, strings.HasPrefix(createdOrganization.Name, "organizations/"))
		require.Equal(t, "Test Organization", createdOrganization.DisplayName)
		require.Equal(t, "test", createdOrganization.Labels["env"])
		require.NotEmpty(t, createdOrganization.Etag)
		require.Nil(t, createdOrganization.DeleteTime)

		createTime := createdOrganization.CreateTime.AsTime()
		require.True(t, !createTime.Before(before))
		require.True(t, !createTime.After(after))

		updateTime := createdOrganization.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before))
		require.True(t, !updateTime.After(after))
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			OrganizationId: "custom-org-id",
			Organization:   validOrganization(),
		}
		createdOrganization, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)
		require.Equal(t, "organizations/custom-org-id", createdOrganization.Name)
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: validOrganization(),
		}
		createdOrganization, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)

		got := getOrganization(t, createdOrganization.Name)
		grpcrequire.Equal(t, createdOrganization, got)
	})

	t.Run("WithoutLabels", func(t *testing.T) {
		t.Parallel()
		organization := validOrganization()
		organization.Labels = nil
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: organization,
		}
		createdOrganization, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)
		require.Empty(t, createdOrganization.Labels)
	})

	t.Run("Protovalidation_MissingOrganization", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{}
		_, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingDisplayName", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: &userpb.Organization{},
		}
		_, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidOrganizationID", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			OrganizationId: "INVALID_UPPERCASE",
			Organization:   validOrganization(),
		}
		_, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestCreate_User(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "Create User Org")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   validUser(),
		}
		createdUser, err := userServiceClient.CreateUser(ctx, createUserRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotEmpty(t, createdUser.Name)
		require.True(t, strings.HasPrefix(createdUser.Name, organization.Name+"/users/"))
		require.Equal(t, "Jane Doe", createdUser.DisplayName)
		require.Equal(t, "jane@example.com", createdUser.EmailAddress)
		require.Equal(t, "+14155551234", createdUser.PhoneNumber)
		require.Equal(t, "admin", createdUser.Labels["role"])
		require.Equal(t, "en", createdUser.Metadata.PreferredLanguage)
		require.Equal(t, "America/New_York", createdUser.Metadata.Timezone)
		require.NotEmpty(t, createdUser.Etag)
		require.Nil(t, createdUser.DeleteTime)

		createTime := createdUser.CreateTime.AsTime()
		require.True(t, !createTime.Before(before))
		require.True(t, !createTime.After(after))

		updateTime := createdUser.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before))
		require.True(t, !updateTime.After(after))
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			UserId: "custom-user-id",
			User:   validUser(),
		}
		createdUser, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)
		require.Equal(t, organization.Name+"/users/custom-user-id", createdUser.Name)
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   validUser(),
		}
		createdUser, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		got := getUser(t, createdUser.Name)
		grpcrequire.Equal(t, createdUser, got)
	})

	t.Run("WithoutOptionalFields", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User: &userpb.User{
				DisplayName:  "Minimal User",
				EmailAddress: "minimal@example.com",
				Metadata:     &userpb.UserMetadata{},
			},
		}
		createdUser, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)
		require.Empty(t, createdUser.PhoneNumber)
		require.Empty(t, createdUser.Labels)
		require.Empty(t, createdUser.Metadata.PreferredLanguage)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			User: validUser(),
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingUser", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingDisplayName", func(t *testing.T) {
		t.Parallel()
		user := validUser()
		user.DisplayName = ""
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   user,
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidEmail", func(t *testing.T) {
		t.Parallel()
		user := validUser()
		user.EmailAddress = "not-an-email"
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   user,
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingEmail", func(t *testing.T) {
		t.Parallel()
		user := validUser()
		user.EmailAddress = ""
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User:   user,
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidUserID", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			UserId: "INVALID_UPPERCASE",
			User:   validUser(),
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
