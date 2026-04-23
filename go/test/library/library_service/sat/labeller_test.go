package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestLabeller_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	labeller, err := aip.NewLabeller[*librarypb.Author](libraryServiceClient)
	require.NoError(t, err)

	t.Run("SetNewLabel", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller SetNew Author")

		labelledAuthor, err := labeller.Label(ctx, author, "env", "production")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"env": "production"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"env": "production"}, got.Labels)
	})

	t.Run("SetLabelOnResourceWithExistingLabels", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labeller Existing Author",
				EmailAddress:   "labeller-existing@example.com",
				EmailAddresses: []string{"labeller-existing@example.com"},
				Labels:         map[string]string{"existing": "value"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		author, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		labelledAuthor, err := labeller.Label(ctx, author, "new-key", "new-value")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"existing": "value", "new-key": "new-value"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"existing": "value", "new-key": "new-value"}, got.Labels)
	})

	t.Run("OverwritesExistingValue", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller Overwrite Author")

		first, err := labeller.Label(ctx, author, "tier", "free")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"tier": "free"}, first.Labels)

		second, err := labeller.Label(ctx, first, "tier", "premium")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"tier": "premium"}, second.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"tier": "premium"}, got.Labels)
	})

	t.Run("SameValueStillWrites", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller SameVal Author")

		first, err := labeller.Label(ctx, author, "env", "prod")
		require.NoError(t, err)

		second, err := labeller.Label(ctx, first, "env", "prod")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"env": "prod"}, second.Labels)
		require.NotEqual(t, first.Etag, second.Etag)
	})

	t.Run("MultipleLabelsSequentially", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller Multi Author")

		first, err := labeller.Label(ctx, author, "env", "prod")
		require.NoError(t, err)

		second, err := labeller.Label(ctx, first, "tier", "gold")
		require.NoError(t, err)

		third, err := labeller.Label(ctx, second, "region", "us-east")
		require.NoError(t, err)

		expectedLabels := map[string]string{"env": "prod", "tier": "gold", "region": "us-east"}
		require.Equal(t, expectedLabels, third.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, expectedLabels, got.Labels)
	})

	t.Run("NilLabelsInitialized", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Labeller NilLabels Author",
				EmailAddress:   "labeller-nil@example.com",
				EmailAddresses: []string{"labeller-nil@example.com"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		author, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Empty(t, author.Labels)

		labelledAuthor, err := labeller.Label(ctx, author, "initialized", "true")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"initialized": "true"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"initialized": "true"}, got.Labels)
	})

	t.Run("NamespacedLabelKey", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller Namespaced Author")

		labelledAuthor, err := labeller.Label(ctx, author, "library.com/status", "approved")
		require.NoError(t, err)
		require.Equal(t, map[string]string{"library.com/status": "approved"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"library.com/status": "approved"}, got.Labels)
	})

	t.Run("PreservesOtherFields", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller Preserve Author")

		labelledAuthor, err := labeller.Label(ctx, author, "tagged", "true")
		require.NoError(t, err)

		require.Equal(t, author.Name, labelledAuthor.Name)
		require.Equal(t, author.DisplayName, labelledAuthor.DisplayName)
		require.Equal(t, author.Biography, labelledAuthor.Biography)
		require.Equal(t, author.EmailAddress, labelledAuthor.EmailAddress)
		require.Equal(t, author.CreateTime.AsTime(), labelledAuthor.CreateTime.AsTime())
		require.Nil(t, labelledAuthor.DeleteTime)
	})

	t.Run("RetryOnAborted_StaleEtag", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller Retry Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      author.Name,
				Biography: "Updated to invalidate etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)

		labelledAuthor, err := labeller.Label(ctx, author, "retried", "true", aip.WithRetryOnAborted[*librarypb.Author]())
		require.NoError(t, err)
		require.Equal(t, map[string]string{"retried": "true"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"retried": "true"}, got.Labels)
		require.Equal(t, "Updated to invalidate etag.", got.Biography)
	})

	t.Run("RetryOnAborted_ReEvaluatesPrecondition", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller RetryPrecon Author")

		first, err := labeller.Label(ctx, author, "version", "v1")
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      first.Name,
				Biography: "Stale the etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err = libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)

		callCount := 0
		precondition := aip.WithPrecondition(func(a *librarypb.Author) bool {
			callCount++
			return a.Labels["version"] == "v1"
		})

		labelledAuthor, err := labeller.Label(ctx, first, "version", "v2", aip.WithRetryOnAborted[*librarypb.Author](), precondition)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"version": "v2"}, labelledAuthor.Labels)
		require.Equal(t, 2, callCount)
	})

	t.Run("WithoutRetry_StaleEtag_Fails", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller NoRetry Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      author.Name,
				Biography: "Invalidate etag without retry.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)

		_, err = labeller.Label(ctx, author, "should-fail", "true")
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("Precondition_SkipIfKeyExists", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller PreconExists Author")

		first, err := labeller.Label(ctx, author, "env", "prod")
		require.NoError(t, err)

		skipIfExists := aip.WithPrecondition(func(a *librarypb.Author) bool {
			_, ok := a.Labels["env"]
			return !ok
		})

		_, err = labeller.Label(ctx, first, "env", "staging", skipIfExists)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"env": "prod"}, got.Labels)
	})

	t.Run("Precondition_SkipIfMatches", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller PreconMatch Author")

		first, err := labeller.Label(ctx, author, "env", "prod")
		require.NoError(t, err)

		skipIfMatches := aip.WithPrecondition(func(a *librarypb.Author) bool {
			v, ok := aip.GetLabel(a, "env")
			return !(ok && v == "prod")
		})

		_, err = labeller.Label(ctx, first, "env", "prod", skipIfMatches)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		skipIfMatches = aip.WithPrecondition(func(a *librarypb.Author) bool {
			v, ok := aip.GetLabel(a, "env")
			return !(ok && v == "staging")
		})

		_, err = labeller.Label(ctx, first, "env", "staging", skipIfMatches)
		require.NoError(t, err)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"env": "staging"}, got.Labels)
	})

	t.Run("Precondition_CheckArbitraryField", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller PreconField Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		rejectDeleted := aip.WithPrecondition(func(a *librarypb.Author) bool {
			return a.DeleteTime == nil
		})

		_, err = labeller.Label(ctx, deletedAuthor, "should-fail", "true", rejectDeleted)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})

	t.Run("Precondition_Passes", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Labeller PreconPass Author")

		requireEmpty := aip.WithPrecondition(func(a *librarypb.Author) bool {
			return len(a.Labels) == 0
		})

		labelledAuthor, err := labeller.Label(ctx, author, "first", "true", requireEmpty)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"first": "true"}, labelledAuthor.Labels)

		got := getAuthor(t, author.Name)
		require.Equal(t, map[string]string{"first": "true"}, got.Labels)
	})
}
