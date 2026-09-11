package outbox

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	pb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

// fakeSchedulerServiceClient records the creates it is handed and fails the
// ones whose payload names a resource in failOn.
type fakeSchedulerServiceClient struct {
	pb.SchedulerServiceClient
	requests []*pb.CreateJobRequest
	failOn   map[string]bool
}

func (c *fakeSchedulerServiceClient) CreateJob(ctx context.Context, request *pb.CreateJobRequest, _ ...grpc.CallOption) (*schedulerpb.Job, error) {
	event := &aippb.ResourceEvent{}
	if err := request.GetJob().GetPayload().UnmarshalTo(event); err != nil {
		return nil, err
	}
	if c.failOn[event.GetName()] {
		return nil, fmt.Errorf("scheduler is down")
	}
	c.requests = append(c.requests, request)
	return &schedulerpb.Job{Name: "jobs/" + request.GetRequestId()}, nil
}

// entry journals a created event of the named resource.
func entry(t *testing.T, id, name string) *Entry {
	t.Helper()
	resource, err := anypb.New(&aippb.ResourceEvent{Name: name})
	require.NoError(t, err)
	event := &aippb.ResourceEvent{
		Name:     name,
		Type:     aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED,
		Resource: resource,
	}
	bytes, err := proto.Marshal(event)
	require.NoError(t, err)
	return &Entry{ID: id, ResourceName: name, EventType: int16(event.GetType()), Event: bytes}
}

// relayOver returns a relay over the entries, and the ids it clears.
func relayOver(client pb.SchedulerServiceClient, entries []*Entry, deleted *[]string) *Relay {
	return New(Opts{
		Name: "test",
		List: func(ctx context.Context, limit int) ([]*Entry, error) {
			if len(entries) < limit {
				limit = len(entries)
			}
			return entries[:limit], nil
		},
		Delete: func(ctx context.Context, ids []string) error {
			*deleted = append(*deleted, ids...)
			entries = entries[len(ids):]
			return nil
		},
		Payload:                func(event *aippb.ResourceEvent) proto.Message { return event },
		SchedulerServiceClient: client,
		BatchSize:              10,
	})
}

func TestRelay_DeliversInOrderAndClears(t *testing.T) {
	ctx := context.Background()
	client := &fakeSchedulerServiceClient{}
	entries := []*Entry{
		entry(t, "01a09000-0000-7000-8000-000000000001", "shelves/a"),
		entry(t, "01a09000-0000-7000-8000-000000000002", "shelves/b"),
	}
	var deleted []string

	require.NoError(t, relayOver(client, entries, &deleted).drain(ctx))

	require.Equal(t, []string{entries[0].ID, entries[1].ID}, deleted)
	require.Len(t, client.requests, 2)
	// The entry id is the create's request id, so a redelivered entry lands on
	// the job it already created.
	require.Equal(t, entries[0].ID, client.requests[0].GetRequestId())
	require.Equal(t, entries[1].ID, client.requests[1].GetRequestId())
}

func TestRelay_HoldsTheJournalAtAFailedEntry(t *testing.T) {
	ctx := context.Background()
	client := &fakeSchedulerServiceClient{failOn: map[string]bool{"shelves/b": true}}
	entries := []*Entry{
		entry(t, "01a09000-0000-7000-8000-000000000001", "shelves/a"),
		entry(t, "01a09000-0000-7000-8000-000000000002", "shelves/b"),
		entry(t, "01a09000-0000-7000-8000-000000000003", "shelves/c"),
	}
	var deleted []string

	err := relayOver(client, entries, &deleted).drain(ctx)

	require.ErrorContains(t, err, "scheduler is down")
	// Only the entry before the failure is delivered and cleared: the one behind
	// it must not overtake the one that failed.
	require.Equal(t, []string{entries[0].ID}, deleted)
	require.Len(t, client.requests, 1)
	require.Equal(t, entries[0].ID, client.requests[0].GetRequestId())
}
