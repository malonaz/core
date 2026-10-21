package sat

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func getLibraryConfig(t *testing.T, name string) *librarypb.LibraryConfig {
	t.Helper()
	libraryConfig, err := libraryServiceClient.GetLibraryConfig(ctx, &libraryservicepb.GetLibraryConfigRequest{Name: name})
	require.NoError(t, err)
	return libraryConfig
}

func updateLibraryConfig(libraryConfig *librarypb.LibraryConfig, paths ...string) (*librarypb.LibraryConfig, error) {
	return libraryServiceClient.UpdateLibraryConfig(ctx, &libraryservicepb.UpdateLibraryConfigRequest{
		LibraryConfig: libraryConfig,
		UpdateMask:    &fieldmaskpb.FieldMask{Paths: paths},
	})
}

func TestLazySingleton_UnwrittenReadsAsDefault(t *testing.T) {
	t.Parallel()
	name := getOrganizationParent() + "/libraryConfig"

	libraryConfig := getLibraryConfig(t, name)
	require.Equal(t, name, libraryConfig.Name)
	require.Nil(t, libraryConfig.CreateTime)
	require.Nil(t, libraryConfig.UpdateTime)
	require.NotEmpty(t, libraryConfig.Etag)
	require.Empty(t, libraryConfig.DisplayName)
	require.Zero(t, libraryConfig.LoanPeriodDays)
	require.Equal(t, libraryConfig.Etag, getLibraryConfig(t, name).Etag)
}

func TestLazySingleton_FirstUpdateCreates(t *testing.T) {
	t.Parallel()
	name := getOrganizationParent() + "/libraryConfig"

	created, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "Main Library"}, "display_name")
	require.NoError(t, err)
	require.Equal(t, "Main Library", created.DisplayName)
	require.NotNil(t, created.CreateTime)
	require.Equal(t, created.CreateTime.AsTime(), created.UpdateTime.AsTime())

	fetched := getLibraryConfig(t, name)
	require.Equal(t, created.Etag, fetched.Etag)
	require.Equal(t, "Main Library", fetched.DisplayName)
}

func TestLazySingleton_LaterUpdatesPatch(t *testing.T) {
	t.Parallel()
	name := getOrganizationParent() + "/libraryConfig"

	created, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "Main Library"}, "display_name")
	require.NoError(t, err)
	updated, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: name, LoanPeriodDays: 14}, "loan_period_days")
	require.NoError(t, err)
	require.Equal(t, "Main Library", updated.DisplayName)
	require.Equal(t, int32(14), updated.LoanPeriodDays)
	require.Equal(t, created.CreateTime.AsTime(), updated.CreateTime.AsTime())
	require.True(t, updated.UpdateTime.AsTime().After(created.UpdateTime.AsTime()))
	require.NotEqual(t, created.Etag, updated.Etag)
}

func TestLazySingleton_StaleEtagAborted(t *testing.T) {
	t.Parallel()
	name := getOrganizationParent() + "/libraryConfig"

	// Unwritten: only the default's etag matches.
	_, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "A", Etag: "stale"}, "display_name")
	grpcrequire.Error(t, codes.Aborted, err)
	defaultEtag := getLibraryConfig(t, name).Etag
	created, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "A", Etag: defaultEtag}, "display_name")
	require.NoError(t, err)

	// Written: the default's etag is now stale.
	_, err = updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "B", Etag: defaultEtag}, "display_name")
	grpcrequire.Error(t, codes.Aborted, err)
	_, err = updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "B", Etag: created.Etag}, "display_name")
	require.NoError(t, err)
}

func TestLazySingleton_ConcurrentFirstUpdatesBothLand(t *testing.T) {
	t.Parallel()
	name := getOrganizationParent() + "/libraryConfig"

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = updateLibraryConfig(&librarypb.LibraryConfig{Name: name, DisplayName: "Main Library"}, "display_name")
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = updateLibraryConfig(&librarypb.LibraryConfig{Name: name, LoanPeriodDays: 21}, "loan_period_days")
	}()
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	libraryConfig := getLibraryConfig(t, name)
	require.Equal(t, "Main Library", libraryConfig.DisplayName)
	require.Equal(t, int32(21), libraryConfig.LoanPeriodDays)
}

func TestLazySingleton_BatchGetFillsDefaults(t *testing.T) {
	t.Parallel()
	unwritten := getOrganizationParent() + "/libraryConfig"
	written := getOrganizationParent() + "/libraryConfig"
	_, err := updateLibraryConfig(&librarypb.LibraryConfig{Name: written, DisplayName: "Main Library"}, "display_name")
	require.NoError(t, err)

	response, err := libraryServiceClient.BatchGetLibraryConfigs(ctx, &libraryservicepb.BatchGetLibraryConfigsRequest{
		Names: []string{unwritten, written},
	})
	require.NoError(t, err)
	require.Len(t, response.LibraryConfigs, 2)
	require.Equal(t, unwritten, response.LibraryConfigs[0].Name)
	require.Nil(t, response.LibraryConfigs[0].CreateTime)
	require.Equal(t, written, response.LibraryConfigs[1].Name)
	require.Equal(t, "Main Library", response.LibraryConfigs[1].DisplayName)
}
