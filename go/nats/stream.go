package nats

import (
	"context"
	"fmt"

	"github.com/malonaz/core/go/pbutil"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

type ServiceStreams struct {
	nameToStream map[string]*Stream
}

type Stream struct {
	*natspb.StreamOptions
}

type Subject struct {
	stream string
	name   string
}

func MustGetServiceStreams(serviceDesc grpc.ServiceDesc) *ServiceStreams {
	streamOptionsList := pbutil.Must(pbutil.GetServiceOption[[]*natspb.StreamOptions](
		serviceDesc.ServiceName,
		natspb.E_Stream,
	))
	nameToStream := make(map[string]*Stream, len(streamOptionsList))
	for _, streamOptions := range streamOptionsList {
		nameToStream[streamOptions.GetName()] = &Stream{StreamOptions: streamOptions}
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

func (s *Stream) Subject(name string) *Subject {
	return &Subject{
		stream: s.GetName(),
		name:   name,
	}
}

func (c *Client) CreateOrUpdateStream(ctx context.Context, streamOptions *Stream) (jetstream.Stream, error) {
	streamConfig := jetstream.StreamConfig{
		Name:     streamOptions.GetName(),
		Subjects: []string{streamOptions.Subject(">").name},
	}
	if maxAge := streamOptions.GetMaxAge(); maxAge != nil {
		streamConfig.MaxAge = maxAge.AsDuration()
	}
	if maxBytes := streamOptions.GetMaxBytes(); maxBytes != 0 {
		streamConfig.MaxBytes = maxBytes
	}
	if maxMsgs := streamOptions.GetMaxMsgs(); maxMsgs != 0 {
		streamConfig.MaxMsgs = maxMsgs
	}
	if maxMsgSize := streamOptions.GetMaxMsgSize(); maxMsgSize != 0 {
		streamConfig.MaxMsgSize = maxMsgSize
	}
	if replicas := streamOptions.GetReplicas(); replicas != 0 {
		streamConfig.Replicas = int(replicas)
	}
	switch streamOptions.GetRetention() {
	case natspb.RetentionPolicy_RETENTION_POLICY_INTEREST:
		streamConfig.Retention = jetstream.InterestPolicy
	case natspb.RetentionPolicy_RETENTION_POLICY_WORK_QUEUE:
		streamConfig.Retention = jetstream.WorkQueuePolicy
	default:
		streamConfig.Retention = jetstream.LimitsPolicy
	}
	switch streamOptions.GetStorage() {
	case natspb.StorageType_STORAGE_TYPE_MEMORY:
		streamConfig.Storage = jetstream.MemoryStorage
	default:
		streamConfig.Storage = jetstream.FileStorage
	}
	switch streamOptions.GetDiscard() {
	case natspb.DiscardPolicy_DISCARD_POLICY_NEW:
		streamConfig.Discard = jetstream.DiscardNew
	default:
		streamConfig.Discard = jetstream.DiscardOld
	}
	stream, err := c.JetStream.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("creating or updating stream %q: %w", streamOptions.GetName(), err)
	}
	c.log.Info(fmt.Sprintf("created or updated stream %q", streamOptions.GetName()))
	return stream, nil
}
