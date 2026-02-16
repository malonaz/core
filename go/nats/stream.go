package nats

import (
	"context"
	"fmt"
	"strings"

	"github.com/malonaz/core/go/pbutil"
	"github.com/nats-io/nats.go/jetstream"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

func MustGetStreamOptions(serviceName string, streamName string) *natspb.StreamOptions {
	streamOptionsList := pbutil.Must(pbutil.GetServiceOption[[]*natspb.StreamOptions](
		serviceName,
		natspb.E_Stream,
	))
	for _, streamOptions := range streamOptionsList {
		if streamOptions.GetName() == streamName {
			return streamOptions
		}
	}
	panic(fmt.Sprintf("stream %q not found on service %q", streamName, serviceName))
}

func getStreamSubject(stream, subject string) string {
	return strings.ReplaceAll(strings.ToLower(stream), "_", ".") + "." + subject
}

func (c *Client) CreateOrUpdateStream(ctx context.Context, streamOptions *natspb.StreamOptions) (Stream, error) {
	streamConfig := jetstream.StreamConfig{
		Name:     streamOptions.GetName(),
		Subjects: []string{getStreamSubject(streamOptions.GetName(), ">")},
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
