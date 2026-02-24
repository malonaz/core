package sat

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/malonaz/core/go/grpc/middleware"
	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func validAuthor() *librarypb.Author {
	return &librarypb.Author{
		DisplayName:    "George Orwell",
		Biography:      "English novelist and essayist.",
		EmailAddress:   "george@example.com",
		PhoneNumber:    "+14155551234",
		EmailAddresses: []string{"mytest@gmail.com", "mytest2@gmail.com"},
		PhoneNumbers:   []string{"+33610102030", "+12247704567"},
		Labels:         map[string]string{"genre": "fiction"},
		Metadata: &librarypb.AuthorMetadata{
			Country: "UK",
		},
	}
}

func createTestAuthor(t *testing.T, parent, displayName string) *librarypb.Author {
	t.Helper()
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: parent,
		Author: &librarypb.Author{
			DisplayName:    displayName,
			Biography:      "Test biography.",
			EmailAddress:   "test@example.com",
			PhoneNumber:    "+12025551234",
			EmailAddresses: []string{"mytest@gmail.com", "mytest2@icloud.com"},
			Metadata: &librarypb.AuthorMetadata{
				Country: "US",
			},
		},
	}
	author, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)
	return author
}

func TestAuthorCreate(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: validAuthor(),
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.NotEmpty(t, createdAuthor.Name)
		require.True(t, strings.HasPrefix(createdAuthor.Name, organizationParent+"/authors/"))
		require.NotNil(t, createdAuthor.CreateTime)
		require.NotNil(t, createdAuthor.UpdateTime)
		require.Nil(t, createdAuthor.DeleteTime)
		require.Equal(t, "George Orwell", createdAuthor.DisplayName)
		require.Equal(t, "UK", createdAuthor.Metadata.Country)
		require.NotEmpty(t, createdAuthor.Etag)
	})

	t.Run("WithCustomID", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:   organizationParent,
			AuthorId: "custom-author-id",
			Author:   validAuthor(),
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, organizationParent+"/authors/custom-author-id", createdAuthor.Name)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Author: validAuthor(),
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingAuthor", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DisplayNameTooShort", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.DisplayName = ""
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidEmail", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.EmailAddress = "not-an-email"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingEmail", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.EmailAddress = ""
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidAuthorID", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:   organizationParent,
			AuthorId: "INVALID_UPPERCASE",
			Author:   validAuthor(),
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestAuthorCanonicalize(t *testing.T) {
	author := validAuthor()
	author.EmailAddress = "TEST@EXAMPLE.COM"
	t.Run("EmailNormalized", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "test@example.com", createdAuthor.EmailAddress)
		// Get the author to verify the canonicalize has been persisted.
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "test@example.com", gotAuthor.EmailAddress)
	})

	t.Run("PhoneNumberE164_US", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.PhoneNumber = "(202) 555-1234"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+12025551234", createdAuthor.PhoneNumber)
		// Get the author to verify the canonicalize has been persisted.
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+12025551234", gotAuthor.PhoneNumber)
	})

	t.Run("PhoneNumberE164_French", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.PhoneNumber = "+33 1 42 68 53 00"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+33142685300", createdAuthor.PhoneNumber)
	})

	t.Run("PhoneNumberE164_FrenchMobile", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.PhoneNumber = "+33 6 12 34 56 78"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+33612345678", createdAuthor.PhoneNumber)
	})

	t.Run("MetadataEmailsNormalized", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.EmailAddresses = []string{"ALICE@EXAMPLE.COM", "BOB@EXAMPLE.COM"}
		author.PhoneNumbers = []string{"(415) 555-9876"}
		author.Metadata.EmailAddresses = []string{"ALICE@EXAMPLE.COM", "BOB@EXAMPLE.COM"}
		author.Metadata.PhoneNumbers = []string{"(415) 555-9876"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, createdAuthor.EmailAddresses)
		require.Equal(t, "+14155559876", createdAuthor.PhoneNumbers[0])
		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, createdAuthor.Metadata.EmailAddresses)
		require.Equal(t, "+14155559876", createdAuthor.Metadata.PhoneNumbers[0])

		// Get the author to verify the canonicalize has been persisted.
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, gotAuthor.EmailAddresses)
		require.Equal(t, "+14155559876", gotAuthor.PhoneNumbers[0])
		require.Equal(t, []string{"alice@example.com", "bob@example.com"}, gotAuthor.Metadata.EmailAddresses)
		require.Equal(t, "+14155559876", gotAuthor.Metadata.PhoneNumbers[0])
	})

	t.Run("MetadataPhoneNumbers_International", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.Metadata.PhoneNumbers = []string{"+33 1 42 68 53 00", "+44 20 7946 0958"}
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+33142685300", createdAuthor.Metadata.PhoneNumbers[0])
		require.Equal(t, "+442079460958", createdAuthor.Metadata.PhoneNumbers[1])
	})

	t.Run("InvalidPhoneNumberRejected", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.PhoneNumber = "not-a-phone"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UpdateCanonicalizesEmail", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Update Canonicalize Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:         author.Name,
				EmailAddress: "UPDATED@EXAMPLE.COM",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"email_address"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "updated@example.com", updatedAuthor.EmailAddress)
	})

	t.Run("UpdateCanonicalizesPhoneNumber_French", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Update Canon Phone FR Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:        author.Name,
				PhoneNumber: "+33 6 98 76 54 32",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"phone_number"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "+33698765432", updatedAuthor.PhoneNumber)
	})
}

func TestAuthorGet(t *testing.T) {
	organizationParent := getOrganizationParent()
	t.Run("Success", func(t *testing.T) {
		author := createTestAuthor(t, organizationParent, "Get Author")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, gotAuthor.Name)
		require.Equal(t, "Get Author", gotAuthor.DisplayName)
	})

	t.Run("NotFound", func(t *testing.T) {
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: organizationParent + "/authors/nonexistent-author",
		}
		_, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{}
		_, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestAuthorUpdate(t *testing.T) {
	t.Run("AllowedFields", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Update Allowed Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:        author.Name,
				DisplayName: "Updated Display Name",
				Biography:   "Updated biography.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "biography"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated Display Name", updatedAuthor.DisplayName)
		require.Equal(t, "Updated biography.", updatedAuthor.Biography)
	})

	t.Run("UpdateTimeChanges", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Update Time Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      author.Name,
				Biography: "New bio.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.True(t, updatedAuthor.UpdateTime.AsTime().After(author.UpdateTime.AsTime()) ||
			updatedAuthor.UpdateTime.AsTime().Equal(author.UpdateTime.AsTime()))
	})

	t.Run("EtagChanges", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Etag Change Author")
		originalEtag := author.Etag
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      author.Name,
				Biography: "Changed for etag test.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.NotEqual(t, originalEtag, updatedAuthor.Etag)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Unauthorized Name Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name: author.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Unauthorized CreateTime Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name: author.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MetadataPartialUpdate", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name: createdAuthor.Name,
				Metadata: &librarypb.AuthorMetadata{
					Country: "UK",
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.country"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "UK", updatedAuthor.Metadata.Country)
		require.Equal(t, author.Metadata.EmailAddresses, updatedAuthor.Metadata.EmailAddresses)
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Labels Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:   author.Name,
				Labels: map[string]string{"env": "production", "tier": "premium"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "production", updatedAuthor.Labels["env"])
		require.Equal(t, "premium", updatedAuthor.Labels["tier"])
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Biography: "No name set.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyMask", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Empty Mask Author")
		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name: author.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestAuthorDelete(t *testing.T) {
	t.Run("SoftDelete", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Soft Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
		}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedAuthor.DeleteTime)

		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.NotNil(t, gotAuthor.DeleteTime)
	})

	t.Run("SoftDeletedHiddenFromList", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Hidden After Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "Hidden After Delete Author"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)
	})

	t.Run("ShowDeletedRevealsDeleted", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Show Deleted Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:      organizationParent,
			Filter:      `display_name = "Show Deleted Author"`,
			ShowDeleted: true,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.NotNil(t, listAuthorsResponse.Authors[0].DeleteTime)
	})

	t.Run("DeleteWithMatchingEtag", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Etag Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
			Etag: author.Etag,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})

	t.Run("DeleteWithWrongEtag", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Wrong Etag Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
			Etag: `"wrong-etag-value"`,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("Delete twice", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Wrong Etag Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
		_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
		deleteAuthorRequest = &libraryservicepb.DeleteAuthorRequest{
			Name:         author.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing throws error on soft-deletable non existent resource", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name:         organizationParent + "/authors/nonexistent-for-delete",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: organizationParent + "/authors/nonexistent-for-delete-err",
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestAuthorList(t *testing.T) {

	t.Run("BasicList", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.NotNil(t, listAuthorsResponse)
	})

	t.Run("FilterByDisplayName_ExactMatch", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "Unique Filter Author 9876")

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "Unique Filter Author 9876"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.Equal(t, "Unique Filter Author 9876", listAuthorsResponse.Authors[0].DisplayName)
	})

	t.Run("FilterByDisplayName_NotEqual", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "NEQ Filter Author 5432")

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name != "NEQ Filter Author 5432"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEqual(t, "NEQ Filter Author 5432", author.DisplayName)
		}
	})

	t.Run("FilterByMetadataCountry", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.Metadata.Country = "JP-unique-filter-test"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.country = "JP-unique-filter-test"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("FilterWithAND", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.DisplayName = "AND Filter Author"
		author.EmailAddress = "and-filter@example.com"
		author.Biography = "unique-bio-for-and-test"
		author.Metadata.Country = "FR"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "AND Filter Author" AND biography = "unique-bio-for-and-test"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("FilterWithOR", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "OR Filter Author A 111")
		createTestAuthor(t, organizationParent, "OR Filter Author B 222")

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "OR Filter Author A 111" OR display_name = "OR Filter Author B 222"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
	})

	t.Run("FilterWithNOT", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "NOT Filter Author XYZZY")

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `NOT display_name = "NOT Filter Author XYZZY"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEqual(t, "NOT Filter Author XYZZY", author.DisplayName)
		}
	})

	t.Run("FilterWithParentheses", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		author := validAuthor()
		author.DisplayName = "Paren Filter A 7788"
		author.EmailAddress = "paren-a@example.com"
		author.Biography = "paren-bio-unique"
		author.Metadata.Country = "DE"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		author2 := validAuthor()
		author2.DisplayName = "Paren Filter B 7788"
		author2.EmailAddress = "paren-b@example.com"
		author2.Biography = "paren-bio-other"
		author2.Metadata.Country = "DE"
		createAuthorRequest2 := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author2,
		}
		_, err = libraryServiceClient.CreateAuthor(ctx, createAuthorRequest2)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `(display_name = "Paren Filter A 7788" OR display_name = "Paren Filter B 7788") AND metadata.country = "DE"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
	})

	t.Run("OrderByAllowed_DisplayName", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:  organizationParent,
			OrderBy: "display_name asc",
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.NotNil(t, listAuthorsResponse)
	})

	t.Run("OrderByAllowed_CreateTimeDesc", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:  organizationParent,
			OrderBy: "create_time desc",
		}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByNotAllowed", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:  organizationParent,
			OrderBy: "biography asc",
		}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Pagination", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		for i := range 3 {
			createTestAuthor(t, organizationParent, fmt.Sprintf("Paginated Author %d", i))
		}

		var allAuthors []*librarypb.Author
		pageToken := ""
		for {
			listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
				Parent:    organizationParent,
				PageSize:  1,
				PageToken: pageToken,
			}
			listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
			require.NoError(t, err)
			allAuthors = append(allAuthors, listAuthorsResponse.Authors...)
			if listAuthorsResponse.NextPageToken == "" {
				break
			}
			pageToken = listAuthorsResponse.NextPageToken
		}
		require.GreaterOrEqual(t, len(allAuthors), 3)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_PageSizeTooLarge", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 1001,
		}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_NegativePageSize", func(t *testing.T) {
		organizationParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: -1,
		}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestAuthorFilterSubstringMatch(t *testing.T) {
	organizationParent := getOrganizationParent()
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Substring Hemingway TestAuthor",
			EmailAddress:   "hemingway-substring@example.com",
			Biography:      "A farewell to arms unique biography text",
			EmailAddresses: []string{"ernest-sub@literature.com", "papa-sub@writing.org"},
			Metadata: &librarypb.AuthorMetadata{
				Country:        "US-substring-test",
				EmailAddresses: []string{"ernest-sub@literature.com", "papa-sub@writing.org"},
				PhoneNumbers:   []string{"+33142685300"},
			},
			Labels: map[string]string{
				"era":    "modernist-substring-test",
				"region": "north-america-substring",
			},
		},
	}
	_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	t.Run("DisplayName_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "Substring Hemingway*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.DisplayName, "Substring Hemingway"))
		}
	})

	t.Run("DisplayName_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "*TestAuthor"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasSuffix(author.DisplayName, "TestAuthor"))
		}
	})

	t.Run("DisplayName_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "*Hemingway*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.Contains(author.DisplayName, "Hemingway"))
		}
	})

	t.Run("Biography_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `biography = "A farewell to arms*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.Biography, "A farewell to arms"))
		}
	})

	t.Run("Biography_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `biography = "*unique biography text"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasSuffix(author.Biography, "unique biography text"))
		}
	})

	t.Run("Biography_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `biography = "*farewell to arms unique*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.Contains(author.Biography, "farewell to arms unique"))
		}
	})

	t.Run("EmailAddress_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_address = "hemingway-substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.EmailAddress, "hemingway-substring"))
		}
	})

	t.Run("EmailAddress_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_address = "*hemingway-substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("MetadataCountry_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.country = "US-substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.Metadata.Country, "US-substring"))
		}
	})

	t.Run("MetadataCountry_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.country = "*substring-test"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasSuffix(author.Metadata.Country, "substring-test"))
		}
	})

	t.Run("MetadataCountry_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.country = "*substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("LabelValue_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.era = "modernist-substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.Labels["era"], "modernist-substring"))
		}
	})

	t.Run("LabelValue_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.region = "*america-substring"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasSuffix(author.Labels["region"], "america-substring"))
		}
	})

	t.Run("LabelValue_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.era = "*substring*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("MetadataEmailAddresses_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.email_addresses:"ernest-sub*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("MetadataEmailAddresses_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.email_addresses:"*@writing.org"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("MetadataEmailAddresses_BothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.email_addresses:"*-sub@*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Equal(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("NoMatch_TrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "zzz-nonexistent-prefix*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)
	})

	t.Run("NoMatch_LeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "*zzz-nonexistent-suffix"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)
	})
}

func TestAuthorFilterLabels(t *testing.T) {
	organizationParent := getOrganizationParent()
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Labels Filter Author Unique 4471",
			EmailAddress:   "labels-filter-4471@example.com",
			EmailAddresses: []string{"mytest@gmail.com", "mytest2@gmail.com"},
			Labels: map[string]string{
				"environment":    "staging-unique-4471",
				"team":           "backend-unique-4471",
				"cost-center-id": "cc-12345",
			},
			Metadata: &librarypb.AuthorMetadata{},
		},
	}
	_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	t.Run("HasKey", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels:"cost-center-id"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			_, ok := author.Labels["cost-center-id"]
			require.True(t, ok)
		}
	})

	t.Run("KeyValueExact", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.environment = "staging-unique-4471"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.Equal(t, "staging-unique-4471", listAuthorsResponse.Authors[0].Labels["environment"])
	})

	t.Run("KeyValueNotEqual", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.environment != "staging-unique-4471"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEqual(t, "staging-unique-4471", author.Labels["environment"])
		}
	})

	t.Run("KeyValueTrailingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.team = "backend-unique*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasPrefix(author.Labels["team"], "backend-unique"))
		}
	})

	t.Run("KeyValueLeadingWildcard", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.team = "*unique-4471"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.True(t, strings.HasSuffix(author.Labels["team"], "unique-4471"))
		}
	})

	t.Run("KeyValueBothWildcards", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.team = "*unique*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("MultipleLabelsAND", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.environment = "staging-unique-4471" AND labels.team = "backend-unique-4471"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("HasKeyAndValueCombined", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels:"cost-center-id" AND labels.environment = "staging-unique-4471"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})
}

func TestAuthorFilterRepeatedFields(t *testing.T) {
	organizationParent := getOrganizationParent()
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Repeated Fields Author 8833",
			EmailAddress:   "repeated-8833@example.com",
			EmailAddresses: []string{"alice-repeated-8833@lit.com", "bob-repeated-8833@pub.org"},
			PhoneNumbers:   []string{"+33142685300", "+442079460958"},
			Metadata: &librarypb.AuthorMetadata{
				Country:        "repeated-country-8833",
				EmailAddresses: []string{"alice-repeated-8833@lit.com", "bob-repeated-8833@pub.org"},
				PhoneNumbers:   []string{"+33142685300", "+442079460958"},
			},
		},
	}
	_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	t.Run("ExactMatchInRepeatedEmails", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"alice-repeated-8833@lit.com"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("ExactMatchSecondElement", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"bob-repeated-8833@pub.org"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("TrailingWildcardInRepeatedEmails", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"alice-repeated*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("LeadingWildcardInRepeatedEmails", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"*@pub.org"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("BothWildcardsInRepeatedEmails", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"*repeated-8833*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("ExactMatchInRepeatedPhoneNumbers", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `phone_numbers:"+33142685300"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("TrailingWildcardInRepeatedPhones", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `phone_numbers:"+3314*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
	})

	t.Run("NoMatch", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_addresses:"nonexistent-zzz@nowhere.com"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)
	})
}

func TestAuthorFilterPresenceAndHas(t *testing.T) {
	organizationParent := getOrganizationParent()
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Presence Author With Email 6622",
			EmailAddress:   "presence-6622@example.com",
			PhoneNumber:    "+14155556622",
			EmailAddresses: []string{"mytest@gmail.com", "mytest2@gmail.com"},
			Labels:         map[string]string{"tier": "gold-6622"},
			Metadata: &librarypb.AuthorMetadata{
				Country:        "presence-country-6622",
				EmailAddresses: []string{"meta-presence-6622@test.com"},
				PhoneNumbers:   []string{"+33142686622"},
			},
		},
	}
	_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	createAuthorRequestEmpty := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: &librarypb.Author{
			DisplayName:    "Presence Author No Optional 6623",
			EmailAddress:   "presence-6623@example.com",
			EmailAddresses: []string{"mytest@gmail.com"},
			Metadata:       &librarypb.AuthorMetadata{},
		},
	}
	_, err = libraryServiceClient.CreateAuthor(ctx, createAuthorRequestEmpty)
	require.NoError(t, err)

	t.Run("HasEmailAddresses Nested", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.email_addresses:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.EmailAddress)
		}
	})

	t.Run("HasPhoneNumbers Nested", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.phone_numbers:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.EmailAddress)
		}
	})

	t.Run("HasEmailAddress", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `email_address:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.EmailAddress)
		}
	})

	t.Run("HasPhoneNumber", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `phone_number:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.PhoneNumber)
		}
	})

	t.Run("HasBiography", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `biography:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Biography)
		}
	})

	t.Run("HasLabels", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Labels)
		}
	})

	t.Run("HasLabelKey", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels:"tier"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			_, ok := author.Labels["tier"]
			require.True(t, ok)
		}
	})

	t.Run("HasMetadataCountry", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.country:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Metadata.Country)
		}
	})

	t.Run("HasMetadataEmailAddresses", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.email_addresses:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Metadata.EmailAddresses)
		}
	})

	t.Run("HasMetadataPhoneNumbers", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `metadata.phone_numbers:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listAuthorsResponse.Authors), 1)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Metadata.PhoneNumbers)
		}
	})

	t.Run("CombinedPresenceAndEquality", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `phone_number:* AND display_name = "Presence Author With Email 6622"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("NegatedPresence", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `NOT phone_number:*`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.Empty(t, author.PhoneNumber)
		}
	})
}

func TestAuthorFieldMask(t *testing.T) {
	organizationParent := getOrganizationParent()
	t.Run("ReturnsOnlyRequestedFields", func(t *testing.T) {
		author := createTestAuthor(t, organizationParent, "Field Mask Author")
		ctxWithFieldMask := middleware.WithFieldMask(ctx, "name,display_name")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithFieldMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, maskedAuthor.Name)
		require.Equal(t, "Field Mask Author", maskedAuthor.DisplayName)
		require.Empty(t, maskedAuthor.Biography)
		require.Empty(t, maskedAuthor.EmailAddress)
		require.Nil(t, maskedAuthor.Metadata)
	})

	t.Run("WildcardReturnsAllFields", func(t *testing.T) {
		author := createTestAuthor(t, organizationParent, "Wildcard Mask Author")
		ctxWithFieldMask := middleware.WithFieldMask(ctx, "*")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithFieldMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "Wildcard Mask Author", maskedAuthor.DisplayName)
		require.NotNil(t, maskedAuthor.Metadata)
	})

	t.Run("NestedFieldMask", func(t *testing.T) {
		author := createTestAuthor(t, organizationParent, "Nested Mask Author")
		ctxWithFieldMask := middleware.WithFieldMask(ctx, "name,metadata.country")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithFieldMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, maskedAuthor.Name)
		require.Empty(t, maskedAuthor.DisplayName)
		require.NotNil(t, maskedAuthor.Metadata)
		require.Equal(t, "US", maskedAuthor.Metadata.Country)
	})
}
