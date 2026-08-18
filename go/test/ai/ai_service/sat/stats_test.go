package sat

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	intervalpb "google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// newUserParent returns a fresh user, so each test aggregates only its own
// chats: stats queries are unfiltered aggregations over the whole table.
func newUserParent() string {
	return "organizations/" + uuid.MustNewV7().String() +
		"/users/" + uuid.MustNewV7().String()
}

func TestComputeStats(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	chatParent := userParent + "/chats/" + uuid.MustNewV7().String()

	// Two generations in one chat: each persists the user's input message plus
	// the generated assistant message.
	_, err := generate(t, chatParent, newToolSet(), newScriptedUserMessage(t, newAssistantText("one")))
	require.NoError(t, err)
	_, err = generate(t, chatParent, newToolSet(), newScriptedUserMessage(t, newAssistantText("two")))
	require.NoError(t, err)

	computeStatsRequest := &aiservicepb.ComputeStatsRequest{Name: userParent}
	stats, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	require.NoError(t, err)

	require.NotNil(t, stats.GetComputeTime())
	require.Equal(t, int32(1), stats.GetTotals().GetChats().GetCount())
	require.Equal(t, int32(4), stats.GetTotals().GetMessages().GetCount())
	// No granularity requested: totals only, no time series.
	require.Empty(t, stats.GetBuckets())

	// The two assistant messages are attributed to the mock model; the two user
	// messages carry no model and group under the empty breakdown.
	modelToBreakdown := map[string]*aipb.ModelBreakdown{}
	for _, breakdown := range stats.GetTotals().GetMessages().GetModelBreakdowns() {
		modelToBreakdown[breakdown.GetModel()] = breakdown
	}
	require.Len(t, modelToBreakdown, 2)
	require.Equal(t, int32(2), modelToBreakdown[mockModel].GetCount())
	require.Equal(t, int32(2), modelToBreakdown[""].GetCount())

	// Consumption categories stay disjoint and separately reported; the mock
	// provider only reports tokens, so audio and image categories stay unset
	// rather than reporting a zeroed message.
	mockBreakdown := modelToBreakdown[mockModel]
	require.NotNil(t, mockBreakdown.GetInputToken())
	require.NotNil(t, mockBreakdown.GetOutputToken())
	require.Positive(t, mockBreakdown.GetInputToken().GetQuantity())
	require.Positive(t, mockBreakdown.GetOutputToken().GetQuantity())
	require.Nil(t, mockBreakdown.GetInputSecond())
	require.Nil(t, mockBreakdown.GetInputImageToken())
	// Messages with no model carry no consumption at all.
	require.Nil(t, modelToBreakdown[""].GetInputToken())
}

func TestComputeStatsGranularityBuckets(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	chatParent := userParent + "/chats/" + uuid.MustNewV7().String()

	_, err := generate(t, chatParent, newToolSet(), newScriptedUserMessage(t, newAssistantText("one")))
	require.NoError(t, err)

	// A day-granular window around now: everything just written lands in
	// today's bucket, and the bucket totals must match the interval totals.
	now := time.Now().UTC()
	computeStatsRequest := &aiservicepb.ComputeStatsRequest{
		Name: userParent,
		Interval: &intervalpb.Interval{
			StartTime: timestamppb.New(now.AddDate(0, 0, -1)),
			EndTime:   timestamppb.New(now.AddDate(0, 0, 1)),
		},
		Granularity: aipb.StatsGranularity_STATS_GRANULARITY_DAY,
	}
	stats, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	require.NoError(t, err)

	require.Len(t, stats.GetBuckets(), 1)
	bucket := stats.GetBuckets()[0]
	require.Equal(t, stats.GetTotals().GetChats().GetCount(), bucket.GetSnapshot().GetChats().GetCount())
	require.Equal(t, stats.GetTotals().GetMessages().GetCount(), bucket.GetSnapshot().GetMessages().GetCount())
	// Day buckets are exactly 24h wide and truncated to midnight UTC.
	bucketStart := bucket.GetInterval().GetStartTime().AsTime()
	require.Equal(t, bucketStart.AddDate(0, 0, 1), bucket.GetInterval().GetEndTime().AsTime())
	require.Equal(t, bucketStart.Truncate(24*time.Hour), bucketStart)
}

func TestComputeStatsEmptyUser(t *testing.T) {
	t.Parallel()
	computeStatsRequest := &aiservicepb.ComputeStatsRequest{Name: newUserParent()}
	stats, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	require.NoError(t, err)
	require.Zero(t, stats.GetTotals().GetChats().GetCount())
	require.Zero(t, stats.GetTotals().GetMessages().GetCount())
}

func TestComputeStatsOrganization(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	chatParent := userParent + "/chats/" + uuid.MustNewV7().String()
	_, err := generate(t, chatParent, newToolSet(), newScriptedUserMessage(t, newAssistantText("one")))
	require.NoError(t, err)

	// Organization-scoped stats aggregate across the organization's users.
	organization := userParent[:strings.Index(userParent, "/users/")]
	computeStatsRequest := &aiservicepb.ComputeStatsRequest{Name: organization}
	stats, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	require.NoError(t, err)
	require.Equal(t, int32(1), stats.GetTotals().GetChats().GetCount())
	require.Equal(t, int32(2), stats.GetTotals().GetMessages().GetCount())
}

func TestComputeStatsInvalidName(t *testing.T) {
	t.Parallel()
	computeStatsRequest := &aiservicepb.ComputeStatsRequest{Name: "providers/foo"}
	_, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}
