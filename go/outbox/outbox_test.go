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

// migrationPaths are the outbox's migrations, which the tests run against.
var migrationPaths = []string{
	"go/outbox/migrations/outbox/001_journal.sql",
}

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
		for _, migrationPath := range migrationPaths {
			migration, err := os.ReadFile(migrationPath)
			if err != nil {
				panic(fmt.Errorf("reading migration %s: %w", migrationPath, err))
			}
			if _, err := postgresClient.Exec(ctx, string(migration)); err != nil {
				panic(fmt.Errorf("running migration %s: %w", migrationPath, err))
			}
		}
		return m.Run()
	}
	os.Exit(run())
}

// journalMessages empties the journal, then journals the messages in order.
func journalMessages(t *testing.T, messages ...string) {
	t.Helper()
	ctx := context.Background()
	_, err := postgresClient.Exec(ctx, "TRUNCATE "+table)
	require.NoError(t, err)

	subject := nats.NewStream(&natspb.StreamOptions{Name: "test.outbox"}).Subject("created")
	journal := NewJournal(postgresClient)
	for _, message := range messages {
		require.NoError(t, journal.Publish(ctx, subject, wrapperspb.String(message)))
	}
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
	return func(_ context.Context, entries []*Entry) ([]*Entry, error) {
		for _, entry := range entries {
			*claimed = append(*claimed, entry.Payload)
		}
		if len(entries) > n {
			return entries[:n], errors.New("publish failed")
		}
		return entries, nil
	}
}

func TestClaimKeepsWhatWasNotPublished(t *testing.T) {
	ctx := context.Background()
	journalMessages(t, "a", "b", "c")

	// The first claim publishes "a" and fails on "b".
	var claimed [][]byte
	count, err := Claim(ctx, postgresClient, 2, publishUpTo(1, &claimed))
	require.Error(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, payloads(t, "a", "b"), claimed)

	// The next one resumes from "b", in the order the messages were journaled.
	claimed = nil
	count, err = Claim(ctx, postgresClient, 10, publishUpTo(10, &claimed))
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, payloads(t, "b", "c"), claimed)

	count, err = Claim(ctx, postgresClient, 10, publishUpTo(10, &claimed))
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestClaimSkipsLockedEntries(t *testing.T) {
	ctx := context.Background()
	journalMessages(t, "a", "b")

	// While one claim holds "a", another takes "b" rather than waiting.
	var concurrent [][]byte
	count, err := Claim(ctx, postgresClient, 1, func(ctx context.Context, entries []*Entry) ([]*Entry, error) {
		count, err := Claim(ctx, postgresClient, 10, publishUpTo(10, &concurrent))
		require.NoError(t, err)
		require.Equal(t, 1, count)
		return entries, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, payloads(t, "b"), concurrent)
}

func TestSequence(t *testing.T) {
	sequence, err := Sequence(map[string][]string{SequenceHeader: {"42"}})
	require.NoError(t, err)
	require.Equal(t, int64(42), sequence)

	_, err = Sequence(map[string][]string{})
	require.Error(t, err)
}
