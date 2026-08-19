package nats

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

type Stream struct {
	name    string
	options *natspb.StreamOptions
}

func NewStream(streamOptions *natspb.StreamOptions) *Stream {
	return &Stream{
		name:    strings.ReplaceAll(streamOptions.GetName(), ".", "_"),
		options: streamOptions,
	}
}

// Name returns the stream's name on the wire, which is its dotted proto name with the dots
// replaced by underscores, as in malonaz_test_library_v1_shelf.
func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) Subject(suffix string) *Subject {
	return &Subject{
		name:   s.name + "." + suffix,
		stream: s,
	}
}

type Subject struct {
	name   string
	stream *Stream
}

// Name returns the subject's name on the wire, so that a test can select on it without
// reconstructing the mangling [Stream.Name] describes.
func (s *Subject) Name() string {
	return s.name
}

func (c *Client) CreateOrUpdateStream(ctx context.Context, s *Stream) (jetstream.Stream, error) {
	streamConfig := jetstream.StreamConfig{
		Name:     s.name,
		Subjects: []string{s.Subject(">").name},
	}
	if maxAge := s.options.GetMaxAge(); maxAge != nil {
		streamConfig.MaxAge = maxAge.AsDuration()
	}
	if maxBytes := s.options.GetMaxBytes(); maxBytes != 0 {
		streamConfig.MaxBytes = maxBytes
	}
	if maxMsgs := s.options.GetMaxMsgs(); maxMsgs != 0 {
		streamConfig.MaxMsgs = maxMsgs
	}
	if maxMsgSize := s.options.GetMaxMsgSize(); maxMsgSize != 0 {
		streamConfig.MaxMsgSize = maxMsgSize
	}
	if replicas := s.options.GetReplicas(); replicas != 0 {
		streamConfig.Replicas = int(replicas)
	}
	switch s.options.GetRetention() {
	case natspb.RetentionPolicy_RETENTION_POLICY_INTEREST:
		streamConfig.Retention = jetstream.InterestPolicy
	case natspb.RetentionPolicy_RETENTION_POLICY_WORK_QUEUE:
		streamConfig.Retention = jetstream.WorkQueuePolicy
	default:
		streamConfig.Retention = jetstream.LimitsPolicy
	}
	switch s.options.GetStorage() {
	case natspb.StorageType_STORAGE_TYPE_MEMORY:
		streamConfig.Storage = jetstream.MemoryStorage
	default:
		streamConfig.Storage = jetstream.FileStorage
	}
	switch s.options.GetDiscard() {
	case natspb.DiscardPolicy_DISCARD_POLICY_NEW:
		streamConfig.Discard = jetstream.DiscardNew
	default:
		streamConfig.Discard = jetstream.DiscardOld
	}
	stream, err := c.JetStream.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("creating or updating stream %q: %w", s.options.GetName(), err)
	}
	c.log.Info(fmt.Sprintf("created or updated stream %q", s.options.GetName()))
	return stream, nil
}
