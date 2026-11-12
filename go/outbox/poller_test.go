package outbox

import (
	"context"
	"net"
	"testing"
	"time"

	natstestserver "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"

	natspb "github.com/malonaz/core/genproto/nats/v1"
	"github.com/malonaz/core/go/nats"
)

// newNatsClient starts an in-process JetStream server and returns a client connected to it.
func newNatsClient(t *testing.T, opts nats.Opts) *nats.Client {
	t.Helper()
	options := natstestserver.DefaultTestOptions
	options.Port = -1
	options.JetStream = true
	options.StoreDir = t.TempDir()
	server := natstestserver.RunServer(&options)
	t.Cleanup(server.Shutdown)

	opts.Host = options.Host
	opts.Port = server.Addr().(*net.TCPAddr).Port
	opts.TotalWait = 5 * time.Second
	opts.ReconnectDelay = time.Second
	client, err := nats.NewClient(&opts)
	require.NoError(t, err)
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(client.Close)
	return client
}

// fakeJournal is an in-memory journal claimed the way Claim claims a table.
type fakeJournal struct {
	entries []*Entry
}

func (j *fakeJournal) claim(ctx context.Context, limit int, publish PublishFn) (int, error) {
	claimed := j.entries[:min(limit, len(j.entries))]
	published, err := publish(ctx, claimed)
	isPublished := make(map[*Entry]bool, len(published))
	for _, entry := range published {
		isPublished[entry] = true
	}
	remaining := make([]*Entry, 0, len(j.entries))
	for _, entry := range j.entries {
		if !isPublished[entry] {
			remaining = append(remaining, entry)
		}
	}
	j.entries = remaining
	return len(claimed), err
}

// streamPayloads returns the payloads the stream holds, in order.
func streamPayloads(t *testing.T, client *nats.Client, stream *nats.Stream) []string {
	t.Helper()
	ctx := context.Background()
	jetStream, err := client.JetStream.Stream(ctx, stream.Name())
	require.NoError(t, err)
	info, err := jetStream.Info(ctx)
	require.NoError(t, err)
	var payloads []string
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq && info.State.Msgs > 0; sequence++ {
		message, err := jetStream.GetMsg(ctx, sequence)
		require.NoError(t, err)
		payloads = append(payloads, string(message.Data))
	}
	return payloads
}

func setup(t *testing.T, opts nats.Opts) (*nats.Client, *nats.Stream, string) {
	t.Helper()
	client := newNatsClient(t, opts)
	stream := nats.NewStream(&natspb.StreamOptions{Name: "test.outbox"})
	_, err := client.CreateOrUpdateStream(context.Background(), stream)
	require.NoError(t, err)
	return client, stream, stream.Subject("created").Name()
}

func TestPollerDrainsInOrder(t *testing.T) {
	client, stream, subject := setup(t, nats.Opts{})
	journal := &fakeJournal{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		journal.entries = append(journal.entries, &Entry{ID: id, Stream: stream.Name(), Subject: subject, Payload: []byte(id)})
	}
	poller := NewPoller(PollerOpts{Name: "test", Claim: journal.claim, BatchSize: 2}, client)

	require.NoError(t, poller.drain(context.Background()))
	require.Empty(t, journal.entries)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, streamPayloads(t, client, stream))
}

func TestPollerDeduplicatesRepublishedEntry(t *testing.T) {
	client, stream, subject := setup(t, nats.Opts{})
	entry := &Entry{ID: "a", Stream: stream.Name(), Subject: subject, Payload: []byte("a")}
	// The entry was published but its deletion rolled back, so it is claimed again.
	journal := &fakeJournal{entries: []*Entry{entry, entry}}
	poller := NewPoller(PollerOpts{Name: "test", Claim: journal.claim}, client)

	require.NoError(t, poller.drain(context.Background()))
	require.Equal(t, []string{"a"}, streamPayloads(t, client, stream))
}

func TestPollerPublishesPastAFailure(t *testing.T) {
	client, stream, subject := setup(t, nats.Opts{})
	journal := &fakeJournal{entries: []*Entry{
		{ID: "a", Stream: stream.Name(), Subject: subject, Payload: []byte("a")},
		{ID: "b", Stream: "no_such_stream", Subject: subject, Payload: []byte("b")},
		{ID: "c", Stream: stream.Name(), Subject: subject, Payload: []byte("c")},
	}}
	poller := NewPoller(PollerOpts{Name: "test", Claim: journal.claim}, client)

	require.Error(t, poller.drain(context.Background()))
	require.Equal(t, []string{"b"}, []string{journal.entries[0].ID})
	require.Equal(t, []string{"a", "c"}, streamPayloads(t, client, stream))
}

func TestPollerPublishesSequence(t *testing.T) {
	client, stream, subject := setup(t, nats.Opts{})
	journal := &fakeJournal{entries: []*Entry{{Sequence: 42, ID: "a", Stream: stream.Name(), Subject: subject, Payload: []byte("a")}}}
	poller := NewPoller(PollerOpts{Name: "test", Claim: journal.claim}, client)
	require.NoError(t, poller.drain(context.Background()))

	ctx := context.Background()
	jetStream, err := client.JetStream.Stream(ctx, stream.Name())
	require.NoError(t, err)
	message, err := jetStream.GetMsg(ctx, 1)
	require.NoError(t, err)
	sequence, err := Sequence(message.Header)
	require.NoError(t, err)
	require.Equal(t, int64(42), sequence)
}

func TestPollerPausedHoldsJournal(t *testing.T) {
	client, stream, subject := setup(t, nats.Opts{OutboxPaused: true})
	journal := &fakeJournal{entries: []*Entry{{ID: "a", Stream: stream.Name(), Subject: subject, Payload: []byte("a")}}}
	poller := NewPoller(PollerOpts{Name: "test", Claim: journal.claim}, client)

	require.NoError(t, poller.drain(context.Background()))
	require.Len(t, journal.entries, 1)
	require.Empty(t, streamPayloads(t, client, stream))
}
