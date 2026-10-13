package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// Case is named after a Go keyword: these round trips prove every generated layer compiles and
// runs for it.
func TestCase_KeywordNamedResource(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	createCaseRequest := &libraryservicepb.CreateCaseRequest{
		Parent: organizationParent,
		CaseId: "rare-books",
		Case:   &librarypb.Case{DisplayName: "Rare books"},
	}
	created, err := libraryServiceClient.CreateCase(ctx, createCaseRequest)
	require.NoError(t, err)
	caseRn, err := librarypb.ParseCaseRn(created.Name)
	require.NoError(t, err)
	require.Equal(t, "rare-books", caseRn.ID())

	created.DisplayName = "Rarest books"
	updateCaseRequest := &libraryservicepb.UpdateCaseRequest{
		Case:       created,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
	updated, err := libraryServiceClient.UpdateCase(ctx, updateCaseRequest)
	require.NoError(t, err)
	require.Equal(t, "Rarest books", updated.DisplayName)

	batchGetCasesRequest := &libraryservicepb.BatchGetCasesRequest{Parent: organizationParent, Names: []string{created.Name}}
	batchGetCasesResponse, err := libraryServiceClient.BatchGetCases(ctx, batchGetCasesRequest)
	require.NoError(t, err)
	require.Len(t, batchGetCasesResponse.Cases, 1)

	deleteCaseRequest := &libraryservicepb.DeleteCaseRequest{Name: created.Name}
	deleted, err := libraryServiceClient.DeleteCase(ctx, deleteCaseRequest)
	require.NoError(t, err)
	require.NotNil(t, deleted.DeleteTime)

	listCasesRequest := &libraryservicepb.ListCasesRequest{Parent: organizationParent}
	listCasesResponse, err := libraryServiceClient.ListCases(ctx, listCasesRequest)
	require.NoError(t, err)
	require.Empty(t, listCasesResponse.Cases)

	undeleteCaseRequest := &libraryservicepb.UndeleteCaseRequest{Name: created.Name}
	undeleted, err := libraryServiceClient.UndeleteCase(ctx, undeleteCaseRequest)
	require.NoError(t, err)
	require.Nil(t, undeleted.DeleteTime)

	getCaseRequest := &libraryservicepb.GetCaseRequest{Name: created.Name}
	got, err := libraryServiceClient.GetCase(ctx, getCaseRequest)
	require.NoError(t, err)
	require.Equal(t, "Rarest books", got.DisplayName)
}
