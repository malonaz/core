package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestAuthorLabels(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("CreateWithLabels", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Create Author",
				EmailAddress:   "labels-create@example.com",
				EmailAddresses: []string{"labels-create@example.com"},
				Labels: map[string]string{
					"env":  "production",
					"tier": "premium",
				},
				Metadata: &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "production", createdAuthor.Labels["env"])
		require.Equal(t, "premium", createdAuthor.Labels["tier"])
	})

	t.Run("CreateWithoutLabels", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "No Labels Author")
		require.Empty(t, author.Labels)
	})

	t.Run("GetPreservesLabels", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Get Author",
				EmailAddress:   "labels-get@example.com",
				EmailAddresses: []string{"labels-get@example.com"},
				Labels:         map[string]string{"region": "us-east"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "us-east", gotAuthor.Labels["region"])
	})

	t.Run("UpdateLabels", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labels Update Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name: author.Name,
				Labels: map[string]string{
					"env":    "staging",
					"region": "eu-west",
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "staging", updatedAuthor.Labels["env"])
		require.Equal(t, "eu-west", updatedAuthor.Labels["region"])
	})

	t.Run("UpdateLabelsReplacesAll", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Replace Author",
				EmailAddress:   "labels-replace@example.com",
				EmailAddresses: []string{"labels-replace@example.com"},
				Labels:         map[string]string{"old-key": "old-value"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:   createdAuthor.Name,
				Labels: map[string]string{"new-key": "new-value"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "new-value", updatedAuthor.Labels["new-key"])
		_, hasOldKey := updatedAuthor.Labels["old-key"]
		require.False(t, hasOldKey)
	})

	t.Run("ClearLabels", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Clear Author",
				EmailAddress:   "labels-clear@example.com",
				EmailAddresses: []string{"labels-clear@example.com"},
				Labels:         map[string]string{"to-remove": "value"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:   createdAuthor.Name,
				Labels: map[string]string{},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Empty(t, updatedAuthor.Labels)
	})

	t.Run("FilterByLabelKeyExists", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Filter Key Author",
				EmailAddress:   "labels-filter-key@example.com",
				EmailAddresses: []string{"labels-filter-key@example.com"},
				Labels:         map[string]string{"unique-label-key-8899": "present"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels:"unique-label-key-8899"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("FilterByLabelKeyValue", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Filter KV Author",
				EmailAddress:   "labels-filter-kv@example.com",
				EmailAddresses: []string{"labels-filter-kv@example.com"},
				Labels:         map[string]string{"status": "unique-active-7766"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.status = "unique-active-7766"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("FilterByLabelKeyValueNotEqual", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels NEQ Author",
				EmailAddress:   "labels-neq@example.com",
				EmailAddresses: []string{"labels-neq@example.com"},
				Labels:         map[string]string{"priority": "unique-high-5544"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.priority != "unique-high-5544"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEqual(t, "unique-high-5544", author.Labels["priority"])
		}
	})

	t.Run("FilterByLabelHasAnyLabels", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels HasAny Author",
				EmailAddress:   "labels-hasany@example.com",
				EmailAddresses: []string{"labels-hasany@example.com"},
				Labels:         map[string]string{"any-key": "any-value"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

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

	t.Run("FilterByLabelWildcardValue", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Wildcard Author",
				EmailAddress:   "labels-wildcard@example.com",
				EmailAddresses: []string{"labels-wildcard@example.com"},
				Labels:         map[string]string{"department": "engineering-platform-unique-3322"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `labels.department = "engineering-platform-unique*"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
	})

	t.Run("LabelsPreservedAfterNonLabelUpdate", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labels Preserved Author",
				EmailAddress:   "labels-preserved@example.com",
				EmailAddresses: []string{"labels-preserved@example.com"},
				Labels:         map[string]string{"keep": "this-value"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:        createdAuthor.Name,
				DisplayName: "Labels Preserved Author Updated",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "this-value", updatedAuthor.Labels["keep"])
	})
}
