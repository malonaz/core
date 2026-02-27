package nats

import (
	"context"
	"fmt"
	"strings"

	"github.com/malonaz/core/go/pbutil"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

type ServiceStreams struct {
	nameToStream map[string]*Stream
}

func MustGetServiceStreams(serviceDesc grpc.ServiceDesc) *ServiceStreams {
	streamOptionsList := pbutil.Must(pbutil.GetServiceOption[[]*natspb.StreamOptions](
		serviceDesc.ServiceName,
		natspb.E_Stream,
	))
	nameToStream := make(map[string]*Stream, len(streamOptionsList))
	for _, streamOptions := range streamOptionsList {
		nameToStream[streamOptions.GetName()] = &Stream{
			name:    strings.ReplaceAll(streamOptions.GetName(), ".", "_"),
			options: streamOptions,
		}
	}
	return &ServiceStreams{nameToStream: nameToStream}
}

func (s *ServiceStreams) GetStreams() []*Stream {
	streams := make([]*Stream, 0, len(s.nameToStream))
	for _, stream := range s.nameToStream {
		streams = append(streams, stream)
	}
	return streams
}

func (s *ServiceStreams) MustGetStream(name string) *Stream {
	stream, ok := s.nameToStream[name]
	if !ok {
		panic(fmt.Sprintf("unknown stream %q", name))
	}
	return stream
}

type Stream struct {
	name    string
	options *natspb.StreamOptions
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
