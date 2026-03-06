// go/test/user/user_service/sat/list_test.go
package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	userservicepb "github.com/malonaz/core/genproto/test/user/user_service/v1"
	userpb "github.com/malonaz/core/genproto/test/user/v1"
)

func TestList_Organization(t *testing.T) {
	t.Parallel()

	t.Run("FilterByDisplayName_ExactMatch", func(t *testing.T) {
		t.Parallel()
		createTestOrganization(t, "Unique Org Filter 55443")

		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			Filter: `display_name = "Unique Org Filter 55443"`,
		}
		listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, listOrganizationsResponse.Organizations, 1)
		require.Equal(t, "Unique Org Filter 55443", listOrganizationsResponse.Organizations[0].DisplayName)
	})

	t.Run("FilterByLabels_HasKey", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: &userpb.Organization{
				DisplayName: "Labeled Org 77665",
				Labels:      map[string]string{"unique-org-key-77665": "present"},
			},
		}
		_, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)

		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			Filter: `labels:"unique-org-key-77665"`,
		}
		listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, listOrganizationsResponse.Organizations, 1)
	})

	t.Run("FilterByLabels_KeyValue", func(t *testing.T) {
		t.Parallel()
		createOrganizationRequest := &userservicepb.CreateOrganizationRequest{
			Organization: &userpb.Organization{
				DisplayName: "Label KV Org 88776",
				Labels:      map[string]string{"status": "unique-active-88776"},
			},
		}
		_, err := userServiceClient.CreateOrganization(ctx, createOrganizationRequest)
		require.NoError(t, err)

		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			Filter: `labels.status = "unique-active-88776"`,
		}
		listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
		require.Len(t, listOrganizationsResponse.Organizations, 1)
	})

	t.Run("OrderByAllowed_DisplayName", func(t *testing.T) {
		t.Parallel()
		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			OrderBy: "display_name asc",
		}
		_, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByAllowed_CreateTimeDesc", func(t *testing.T) {
		t.Parallel()
		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			OrderBy: "create_time desc",
		}
		_, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		require.NoError(t, err)
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()
		for i := range 3 {
			createTestOrganization(t, fmt.Sprintf("Paginated Org %d 99887", i))
		}

		var allOrganizations []*userpb.Organization
		pageToken := ""
		for {
			listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
				Filter:    `display_name = "Paginated Org*" AND display_name = "*99887"`,
				PageSize:  1,
				PageToken: pageToken,
			}
			listOrganizationsResponse, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
			require.NoError(t, err)
			allOrganizations = append(allOrganizations, listOrganizationsResponse.Organizations...)
			if listOrganizationsResponse.NextPageToken == "" {
				break
			}
			pageToken = listOrganizationsResponse.NextPageToken
		}
		require.Len(t, allOrganizations, 3)
	})

	t.Run("Protovalidation_PageSizeTooLarge", func(t *testing.T) {
		t.Parallel()
		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			PageSize: 1001,
		}
		_, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_NegativePageSize", func(t *testing.T) {
		t.Parallel()
		listOrganizationsRequest := &userservicepb.ListOrganizationsRequest{
			PageSize: -1,
		}
		_, err := userServiceClient.ListOrganizations(ctx, listOrganizationsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestList_User(t *testing.T) {
	t.Parallel()
	organization := createTestOrganization(t, "List User Org")

	t.Run("BasicList", func(t *testing.T) {
		t.Parallel()
		createTestUser(t, organization.Name, "List User A")
		createTestUser(t, organization.Name, "List User B")

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listUsersResponse.Users), 2)
	})

	t.Run("FilterByDisplayName_ExactMatch", func(t *testing.T) {
		t.Parallel()
		createTestUser(t, organization.Name, "Unique User Filter 33221")

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `display_name = "Unique User Filter 33221"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
		require.Equal(t, "Unique User Filter 33221", listUsersResponse.Users[0].DisplayName)
	})

	t.Run("FilterByEmailAddress", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User: &userpb.User{
				DisplayName:  "Email Filter User",
				EmailAddress: "unique-email-filter-44332@example.com",
				Metadata:     &userpb.UserMetadata{},
			},
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `email_address = "unique-email-filter-44332@example.com"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
	})

	t.Run("FilterByMetadata", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User: &userpb.User{
				DisplayName:  "Meta Filter User",
				EmailAddress: "meta-filter@example.com",
				Metadata: &userpb.UserMetadata{
					PreferredLanguage: "ja-unique-filter-test",
				},
			},
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `metadata.preferred_language = "ja-unique-filter-test"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
	})

	t.Run("FilterWithAND", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User: &userpb.User{
				DisplayName:  "AND Filter User 55443",
				EmailAddress: "and-filter-55443@example.com",
				Metadata: &userpb.UserMetadata{
					Timezone: "Asia/Tokyo",
				},
			},
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `display_name = "AND Filter User 55443" AND metadata.timezone = "Asia/Tokyo"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
	})

	t.Run("FilterWithOR", func(t *testing.T) {
		t.Parallel()
		createTestUser(t, organization.Name, "OR Filter User A 66554")
		createTestUser(t, organization.Name, "OR Filter User B 66554")

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `display_name = "OR Filter User A 66554" OR display_name = "OR Filter User B 66554"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 2)
	})

	t.Run("FilterPresenceCheck", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `phone_number:*`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		for _, user := range listUsersResponse.Users {
			require.NotEmpty(t, user.PhoneNumber)
		}
	})

	t.Run("FilterByLabels_HasKey", func(t *testing.T) {
		t.Parallel()
		createUserRequest := &userservicepb.CreateUserRequest{
			Parent: organization.Name,
			User: &userpb.User{
				DisplayName:  "Label User 77665",
				EmailAddress: "label-77665@example.com",
				Labels:       map[string]string{"unique-user-key-77665": "yes"},
				Metadata:     &userpb.UserMetadata{},
			},
		}
		_, err := userServiceClient.CreateUser(ctx, createUserRequest)
		require.NoError(t, err)

		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent: organization.Name,
			Filter: `labels:"unique-user-key-77665"`,
		}
		listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
		require.Len(t, listUsersResponse.Users, 1)
	})

	t.Run("OrderByAllowed_DisplayName", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent:  organization.Name,
			OrderBy: "display_name asc",
		}
		_, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByAllowed_CreateTimeDesc", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent:  organization.Name,
			OrderBy: "create_time desc",
		}
		_, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		require.NoError(t, err)
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()
		paginationOrg := createTestOrganization(t, "Pagination User Org")
		for i := range 3 {
			createTestUser(t, paginationOrg.Name, fmt.Sprintf("Paginated User %d", i))
		}

		var allUsers []*userpb.User
		pageToken := ""
		for {
			listUsersRequest := &userservicepb.ListUsersRequest{
				Parent:    paginationOrg.Name,
				PageSize:  1,
				PageToken: pageToken,
			}
			listUsersResponse, err := userServiceClient.ListUsers(ctx, listUsersRequest)
			require.NoError(t, err)
			allUsers = append(allUsers, listUsersResponse.Users...)
			if listUsersResponse.NextPageToken == "" {
				break
			}
			pageToken = listUsersResponse.NextPageToken
		}
		require.Len(t, allUsers, 3)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{}
		_, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_PageSizeTooLarge", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent:   organization.Name,
			PageSize: 1001,
		}
		_, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_NegativePageSize", func(t *testing.T) {
		t.Parallel()
		listUsersRequest := &userservicepb.ListUsersRequest{
			Parent:   organization.Name,
			PageSize: -1,
		}
		_, err := userServiceClient.ListUsers(ctx, listUsersRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}
