package teststore_test

import (
	"context"
	"net"
	"testing"
	"time"

	natstestserver "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	corenats "github.com/malonaz/core/go/nats"
	teststore "github.com/malonaz/core/go/nats/test_store"

	aippb "github.com/malonaz/core/genproto/aip/v1"
)

// pollInterval is how often the assertions below re-check the buffer, which fills on the
// subscription's goroutine.
const pollInterval = 10 * time.Millisecond

// newNatsClient starts an in-process NATS server and returns a client connected to it.
func newNatsClient(t *testing.T) *corenats.Client {
	t.Helper()
	options := natstestserver.DefaultTestOptions
	options.Port = -1
	server := natstestserver.RunServer(&options)
	t.Cleanup(server.Shutdown)

	client, err := corenats.NewClient(&corenats.Opts{
		Host:           options.Host,
		Port:           server.Addr().(*net.TCPAddr).Port,
		TotalWait:      5 * time.Second,
		ReconnectDelay: time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(client.Close)
	return client
}

// publish marshals and publishes an event, returning once the server has it.
func publish(t *testing.T, client *corenats.Client, subject string, event *aippb.ResourceEvent) {
	t.Helper()
	payload, err := proto.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, client.Conn.Publish(subject, payload))
	require.NoError(t, client.Conn.Flush())
}

// createdEvent returns the event published when a resource of the given name is created.
func createdEvent(name string) *aippb.ResourceEvent {
	return &aippb.ResourceEvent{
		Name: name,
		Type: aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED,
	}
}

func TestNatsRequire(t *testing.T) {
	client := newNatsClient(t)
	natsStore, err := teststore.NewNats(client, "test.>")
	require.NoError(t, err)
	t.Cleanup(natsStore.Close)

	publish(t, client, "test.one.created", createdEvent("organizations/one"))

	// Wildcards select the message just as the literal subject does.
	event := natsStore.RequireWithTimeout(t, "test.*.created", 2*time.Second)
	require.Equal(t, "organizations/one", event.GetName())
	require.Equal(t, aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED, event.GetType())

	// It was consumed, so nothing is left on any subject.
	require.Empty(t, natsStore.Messages("test.>"))
}

func TestNatsRequireIgnoresOtherSubjects(t *testing.T) {
	client := newNatsClient(t)
	natsStore, err := teststore.NewNats(client, "test.>")
	require.NoError(t, err)
	t.Cleanup(natsStore.Close)

	publish(t, client, "test.two.created", createdEvent("organizations/two"))
	require.Eventually(t, func() bool { return len(natsStore.Messages("test.>")) == 1 }, 2*time.Second, pollInterval)

	// A message on a subject the pattern does not cover is not a candidate, so Require would
	// time out on it. Asserted through Messages rather than by calling Require, which aborts.
	require.Empty(t, natsStore.Messages("test.one.created"))
	require.Empty(t, natsStore.Messages("test.two.updated"))
	require.Empty(t, natsStore.Messages("test.two"))

	event := natsStore.RequireWithTimeout(t, "test.two.created", 2*time.Second)
	require.Equal(t, "organizations/two", event.GetName())
}

func TestNatsMessages(t *testing.T) {
	client := newNatsClient(t)
	natsStore, err := teststore.NewNats(client, "test.>")
	require.NoError(t, err)
	t.Cleanup(natsStore.Close)

	publish(t, client, "test.one.created", createdEvent("organizations/one"))
	publish(t, client, "test.two.created", createdEvent("organizations/two"))
	require.Eventually(t, func() bool { return len(natsStore.Messages("test.>")) == 2 }, 2*time.Second, pollInterval)
	require.Len(t, natsStore.Messages("test.one.created"), 1)

	message := natsStore.Messages("test.one.created")[0]
	require.Equal(t, "test.one.created", message.Subject)
	buffered := &aippb.ResourceEvent{}
	require.NoError(t, message.Unmarshal(buffered))
	require.Equal(t, "organizations/one", buffered.GetName())

	natsStore.Reset()
	require.Empty(t, natsStore.Messages("test.>"))
}
