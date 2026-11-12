package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	natspb "github.com/malonaz/core/genproto/nats/v1"
	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
)

var postgresClient *postgres.Client

func TestMain(m *testing.M) {
	run := func() int {
		ctx := context.Background()
		server := testserver.NewServer(testserver.Config{})
		if err := server.Start(ctx); err != nil {
			panic(fmt.Errorf("starting postgres: %w", err))
		}
		defer server.Shutdown()
		postgresClient = server.Client()
		if err := postgresClient.Start(ctx); err != nil {
			panic(fmt.Errorf("connecting to postgres: %w", err))
		}
		return m.Run()
	}
	os.Exit(run())
}

// newJournalTable creates an empty journal table holding the given messages, journaled in order.
func newJournalTable(t *testing.T, name string, messages ...string) string {
	t.Helper()
	ctx := context.Background()
	_, err := postgresClient.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		id UUID NOT NULL DEFAULT gen_random_uuid(),
		create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
		stream TEXT NOT NULL,
		subject TEXT NOT NULL,
		payload BYTEA NOT NULL
	)`, name))
	require.NoError(t, err)

	subject := nats.NewStream(&natspb.StreamOptions{Name: "test.outbox"}).Subject("created")
	journal := NewJournal(postgresClient, name)
	for _, message := range messages {
		require.NoError(t, journal.Publish(ctx, subject, wrapperspb.String(message)))
	}
	return name
}

// payloads returns what the messages are journaled as.
func payloads(t *testing.T, messages ...string) [][]byte {
	t.Helper()
	payloads := make([][]byte, 0, len(messages))
	for _, message := range messages {
		payload, err := nats.Encode(wrapperspb.String(message))
		require.NoError(t, err)
		payloads = append(payloads, payload)
	}
	return payloads
}

// publishUpTo returns a PublishFn recording the payloads it is handed and publishing only the first n of them.
func publishUpTo(n int, claimed *[][]byte) PublishFn {
	return func(_ context.Context, entries []*Entry) (int, error) {
		for _, entry := range entries {
			*claimed = append(*claimed, entry.Payload)
		}
		if len(entries) > n {
			return n, errors.New("publish failed")
		}
		return len(entries), nil
	}
}

func TestClaimKeepsWhatWasNotPublished(t *testing.T) {
	ctx := context.Background()
	table := newJournalTable(t, "journal_partial", "a", "b", "c")

	// The first claim publishes "a" and fails on "b".
	var claimed [][]byte
	count, err := Claim(ctx, postgresClient, table, 2, publishUpTo(1, &claimed))
	require.Error(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, payloads(t, "a", "b"), claimed)

	// The next one resumes from "b", in the order the messages were journaled.
	claimed = nil
	count, err = Claim(ctx, postgresClient, table, 10, publishUpTo(10, &claimed))
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, payloads(t, "b", "c"), claimed)

	count, err = Claim(ctx, postgresClient, table, 10, publishUpTo(10, &claimed))
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestClaimHoldsTheJournal(t *testing.T) {
	ctx := context.Background()
	table := newJournalTable(t, "journal_exclusive", "a", "b")

	// While one claim publishes, another finds nothing to overtake it with.
	var concurrent [][]byte
	count, err := Claim(ctx, postgresClient, table, 1, func(ctx context.Context, entries []*Entry) (int, error) {
		count, err := Claim(ctx, postgresClient, table, 10, publishUpTo(10, &concurrent))
		require.NoError(t, err)
		require.Zero(t, count)
		return len(entries), nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Empty(t, concurrent)
}
