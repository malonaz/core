package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/nats"
)

const retryDelay = 5 * time.Second

// Generate runs T's analyses on a resource, all of them or those named by id, and records the
// results. Analyses that don't apply to the resource are skipped; current results are reused.
func Generate[T proto.Message](ctx context.Context, registry *Registry, resourceName string, ids ...string) ([]*analysispb.Result, error) {
	typeName, err := ResourceType[T]()
	if err != nil {
		return nil, err
	}
	rt, ok := registry.typeToResource[typeName]
	if !ok {
		return nil, fmt.Errorf("%s: not registered", typeName)
	}
	if len(ids) == 0 {
		ids = rt.ids
	}
	analyses := make([]*registered, 0, len(ids))
	for _, id := range ids {
		analysis, ok := rt.idToAnalysis[id]
		if !ok {
			return nil, fmt.Errorf("%s/%s: not registered", typeName, id)
		}
		analyses = append(analyses, analysis)
	}
	results, stats, err := registry.run(ctx, rt, resourceName, analyses, true)
	log.InfoContext(ctx, "analysed", "resource", resourceName, "results", len(results), "current", stats.current, "skipped", stats.skipped, "calls", stats.calls)
	return results, err
}

// HandleEvents returns a processor func running T's analyses on each created or updated T.
// Subscribe it to the resource's own event subjects.
func HandleEvents[T proto.Message](registry *Registry) nats.ProcessorFunc[*aippb.ResourceEvent] {
	return func(ctx context.Context, message *nats.Message[*aippb.ResourceEvent]) error {
		event := message.Payload
		switch event.GetType() {
		case aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED:
		default:
			return nil
		}
		if recordedOnly(event) {
			return nil
		}
		_, err := Generate[T](ctx, registry, event.GetName())
		if err == nil {
			return nil
		}
		err = fmt.Errorf("analysing %s: %w", event.GetName(), err)
		// Retrying won't fix these.
		if status.HasCode(err, codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound) {
			log.ErrorContext(ctx, "analysing", "resource", event.GetName(), "error", err)
			return nats.TermError(err)
		}
		return nats.NakWithDelayError(err, retryDelay)
	}
}

// recordedOnly reports whether an update only recorded analysis results, so has nothing to analyse.
func recordedOnly(event *aippb.ResourceEvent) bool {
	paths := event.GetUpdateMask().GetPaths()
	if event.GetType() != aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED || len(paths) == 0 {
		return false
	}
	prefix := analysispb.Labels.Analysis.GetKey("")
	for _, path := range paths {
		field, key, ok := strings.Cut(path, ".")
		if !ok || (field != "labels" && field != "annotations") || !strings.HasPrefix(strings.Trim(key, "`"), prefix) {
			return false
		}
	}
	return true
}

// ListPage lists one page of resource names.
type ListPage func(ctx context.Context, pageToken string) (names []string, nextPageToken string, err error)

// Backfill runs Generate on every resource list returns, concurrency at a time, and returns
// how many it ran on. List them all first: recording changes labels a filter may read.
// Filter on the label to skip analysed resources, e.g. `-labels."analysis.malonaz.com/polite":*`.
func Backfill[T proto.Message](ctx context.Context, registry *Registry, list ListPage, concurrency int, ids ...string) (int, error) {
	var names []string
	for pageToken := ""; ; {
		page, next, err := list(ctx, pageToken)
		if err != nil {
			return 0, fmt.Errorf("listing: %w", err)
		}
		names = append(names, page...)
		if next == "" {
			break
		}
		pageToken = next
	}
	var mutex sync.Mutex
	var errs []error
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for _, name := range names {
		group.Go(func() error {
			if _, err := Generate[T](ctx, registry, name, ids...); err != nil {
				mutex.Lock()
				defer mutex.Unlock()
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
			}
			return nil
		})
	}
	group.Wait()
	return len(names), errors.Join(errs...)
}
