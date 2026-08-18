package ai_service

import (
	"context"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"
	intervalpb "google.golang.org/genproto/googleapis/type/interval"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/model"
	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

var granularityToDateTruncField = map[aipb.StatsGranularity]string{
	aipb.StatsGranularity_STATS_GRANULARITY_DAY:   "day",
	aipb.StatsGranularity_STATS_GRANULARITY_WEEK:  "week",
	aipb.StatsGranularity_STATS_GRANULARITY_MONTH: "month",
}

// ComputeStats aggregates a user's or an organization's chats and messages
// into interval totals and, when a granularity is requested, a time series.
// Nothing is stored: the response is computed per request.
func (s *Service) ComputeStats(ctx context.Context, request *pb.ComputeStatsRequest) (*pb.ComputeStatsResponse, error) {
	// The name is polymorphic: a user or an organization. An empty user ID
	// tells the store to aggregate across the whole organization.
	var organizationID, userID string
	userRn := &aipb.UserResourceName{}
	organizationRn := &aipb.OrganizationResourceName{}
	switch {
	case userRn.UnmarshalString(request.GetName()) == nil:
		organizationID, userID = userRn.Organization, userRn.User
	case organizationRn.UnmarshalString(request.GetName()) == nil:
		organizationID = organizationRn.Organization
	default:
		return nil, status.Errorf(codes.InvalidArgument, "name must be a user or an organization resource name").Err()
	}

	var startTime, endTime *time.Time
	if request.GetInterval().GetStartTime() != nil {
		t := request.GetInterval().GetStartTime().AsTime()
		startTime = &t
	}
	if request.GetInterval().GetEndTime() != nil {
		t := request.GetInterval().GetEndTime().AsTime()
		endTime = &t
	}
	granularity := request.GetGranularity()
	dateTruncField := granularityToDateTruncField[granularity]

	var (
		chatStatsRows    []*model.ChatStatsRow
		messageStatsRows []*model.MessageStatsRow
	)
	eg, ctxEg := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		chatStatsRows, err = s.aiPostgresStore.ComputeChatStats(ctxEg, organizationID, userID, startTime, endTime, dateTruncField)
		return err
	})
	eg.Go(func() error {
		var err error
		messageStatsRows, err = s.aiPostgresStore.ComputeMessageStats(ctxEg, organizationID, userID, startTime, endTime, dateTruncField)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "computing stats: %v", err).Err()
	}

	// Totals are folded from the same rows as the buckets: when a granularity is
	// requested each row feeds both its bucket's aggregator and the totals one,
	// so the database is queried exactly once per resource kind.
	totalsAggregator := newStatsAggregator()
	bucketStartToAggregator := map[time.Time]*statsAggregator{}
	aggregatorsFor := func(bucket *time.Time) []*statsAggregator {
		aggregators := []*statsAggregator{totalsAggregator}
		if bucket == nil {
			return aggregators
		}
		aggregator, ok := bucketStartToAggregator[*bucket]
		if !ok {
			aggregator = newStatsAggregator()
			bucketStartToAggregator[*bucket] = aggregator
		}
		return append(aggregators, aggregator)
	}

	for _, row := range chatStatsRows {
		for _, aggregator := range aggregatorsFor(row.Bucket) {
			aggregator.addChatStatsRow(row)
		}
	}
	for _, row := range messageStatsRows {
		for _, aggregator := range aggregatorsFor(row.Bucket) {
			aggregator.addMessageStatsRow(row)
		}
	}

	response := &pb.ComputeStatsResponse{
		Interval:    request.GetInterval(),
		ComputeTime: timestamppb.Now(),
		Totals:      totalsAggregator.snapshot(),
	}

	bucketStarts := make([]time.Time, 0, len(bucketStartToAggregator))
	for bucketStart := range bucketStartToAggregator {
		bucketStarts = append(bucketStarts, bucketStart)
	}
	sort.Slice(bucketStarts, func(i, j int) bool { return bucketStarts[i].Before(bucketStarts[j]) })
	for _, bucketStart := range bucketStarts {
		response.Buckets = append(response.Buckets, &aipb.StatsBucket{
			Interval: &intervalpb.Interval{
				StartTime: timestamppb.New(bucketStart),
				EndTime:   timestamppb.New(bucketEndTime(bucketStart, granularity)),
			},
			Snapshot: bucketStartToAggregator[bucketStart].snapshot(),
		})
	}

	return response, nil
}

// statsAggregator folds aggregated rows into one snapshot; one instance
// backs the totals and one backs each time bucket.
type statsAggregator struct {
	chatCount        int32
	chatPrice        float64
	messageCount     int32
	messagePrice     float64
	modelToAggregate map[string]*modelAggregate
}

// modelAggregate accumulates one model's messages. Consumption is kept
// per-category (never folded) so the response mirrors ModelUsage exactly.
type modelAggregate struct {
	count       int32
	price       float64
	consumption map[string]*consumptionAggregate
}

type consumptionAggregate struct {
	quantity int64
	price    float64
}

// consumptionCategory names a ModelUsage category and wires the row columns it
// aggregates to the response field it populates, so adding a category is a
// one-line change instead of three parallel edits.
type consumptionCategory struct {
	name     string
	quantity func(*model.MessageStatsRow) int64
	price    func(*model.MessageStatsRow) float64
	set      func(*aipb.ModelBreakdown, *aipb.ResourceConsumptionStats)
}

var consumptionCategories = []consumptionCategory{{
	name:     "input_token",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputTokenQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputTokenPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputToken = c },
}, {
	name:     "output_token",
	quantity: func(r *model.MessageStatsRow) int64 { return r.OutputTokenQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.OutputTokenPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.OutputToken = c },
}, {
	name:     "output_reasoning_token",
	quantity: func(r *model.MessageStatsRow) int64 { return r.OutputReasoningTokenQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.OutputReasoningTokenPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.OutputReasoningToken = c },
}, {
	name:     "input_token_cache_read",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputTokenCacheReadQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputTokenCacheReadPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputTokenCacheRead = c },
}, {
	name:     "input_token_cache_write",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputTokenCacheWriteQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputTokenCacheWritePrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputTokenCacheWrite = c },
}, {
	name:     "input_second",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputSecondQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputSecondPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputSecond = c },
}, {
	name:     "output_second",
	quantity: func(r *model.MessageStatsRow) int64 { return r.OutputSecondQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.OutputSecondPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.OutputSecond = c },
}, {
	name:     "input_character",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputCharacterQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputCharacterPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputCharacter = c },
}, {
	name:     "input_image_token",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputImageTokenQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputImageTokenPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputImageToken = c },
}, {
	name:     "output_image_token",
	quantity: func(r *model.MessageStatsRow) int64 { return r.OutputImageTokenQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.OutputImageTokenPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.OutputImageToken = c },
}, {
	name:     "input_image_token_cache_read",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputImageTokenCacheReadQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputImageTokenCacheReadPrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputImageTokenCacheRead = c },
}, {
	name:     "input_image_token_cache_write",
	quantity: func(r *model.MessageStatsRow) int64 { return r.InputImageTokenCacheWriteQuantity },
	price:    func(r *model.MessageStatsRow) float64 { return r.InputImageTokenCacheWritePrice },
	set:      func(b *aipb.ModelBreakdown, c *aipb.ResourceConsumptionStats) { b.InputImageTokenCacheWrite = c },
}}

func newStatsAggregator() *statsAggregator {
	return &statsAggregator{
		modelToAggregate: map[string]*modelAggregate{},
	}
}

func (a *statsAggregator) addChatStatsRow(row *model.ChatStatsRow) {
	a.chatCount += row.Count
	a.chatPrice += row.Price
}

func (a *statsAggregator) addMessageStatsRow(row *model.MessageStatsRow) {
	a.messageCount += row.Count
	a.messagePrice += row.Price
	aggregate, ok := a.modelToAggregate[row.Model]
	if !ok {
		aggregate = &modelAggregate{consumption: map[string]*consumptionAggregate{}}
		a.modelToAggregate[row.Model] = aggregate
	}
	aggregate.count += row.Count
	aggregate.price += row.Price
	for _, category := range consumptionCategories {
		consumption, ok := aggregate.consumption[category.name]
		if !ok {
			consumption = &consumptionAggregate{}
			aggregate.consumption[category.name] = consumption
		}
		consumption.quantity += category.quantity(row)
		consumption.price += category.price(row)
	}
}

func (a *statsAggregator) snapshot() *aipb.StatsSnapshot {
	// Sorted by model resource name for a deterministic response.
	models := make([]string, 0, len(a.modelToAggregate))
	for modelName := range a.modelToAggregate {
		models = append(models, modelName)
	}
	sort.Strings(models)

	messageStats := &aipb.MessageStats{
		Count: a.messageCount,
		Price: a.messagePrice,
	}
	for _, modelName := range models {
		aggregate := a.modelToAggregate[modelName]
		breakdown := &aipb.ModelBreakdown{
			Model: modelName,
			Count: aggregate.count,
			Price: aggregate.price,
		}
		for _, category := range consumptionCategories {
			consumption := aggregate.consumption[category.name]
			// Unused categories stay unset rather than reporting a zeroed
			// message: a text model should not claim it consumed audio.
			if consumption == nil || (consumption.quantity == 0 && consumption.price == 0) {
				continue
			}
			category.set(breakdown, &aipb.ResourceConsumptionStats{
				Quantity: consumption.quantity,
				Price:    consumption.price,
			})
		}
		messageStats.ModelBreakdowns = append(messageStats.ModelBreakdowns, breakdown)
	}
	return &aipb.StatsSnapshot{
		Chats: &aipb.ChatStats{
			Count: a.chatCount,
			Price: a.chatPrice,
		},
		Messages: messageStats,
	}
}

func bucketEndTime(bucketStart time.Time, granularity aipb.StatsGranularity) time.Time {
	switch granularity {
	case aipb.StatsGranularity_STATS_GRANULARITY_DAY:
		return bucketStart.AddDate(0, 0, 1)
	case aipb.StatsGranularity_STATS_GRANULARITY_WEEK:
		return bucketStart.AddDate(0, 0, 7)
	case aipb.StatsGranularity_STATS_GRANULARITY_MONTH:
		return bucketStart.AddDate(0, 1, 0)
	}
	return bucketStart
}
