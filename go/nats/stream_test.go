package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

func TestStreamConfigReplicas(t *testing.T) {
	stream := func(replicas int32) *Stream {
		return NewStream(&natspb.StreamOptions{
			Name:     "malonaz.test.library.v1.book",
			MaxAge:   durationpb.New(720 * time.Hour),
			Replicas: replicas,
		})
	}
	config := func(replicas int) jetstream.StreamConfig {
		return jetstream.StreamConfig{
			Name:      "malonaz_test_library_v1_book",
			Subjects:  []string{"malonaz_test_library_v1_book.>"},
			MaxAge:    720 * time.Hour,
			Replicas:  replicas,
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.FileStorage,
			Discard:   jetstream.DiscardOld,
		}
	}
	single := &Client{opts: &Opts{}}
	clustered := &Client{opts: &Opts{StreamReplicas: 3}}

	// The server's default without either.
	require.Equal(t, config(0), single.streamConfig(stream(0)))
	// The client's where the options set none.
	require.Equal(t, config(3), clustered.streamConfig(stream(0)))
	// The options' own over the client's.
	require.Equal(t, config(5), clustered.streamConfig(stream(5)))
}
