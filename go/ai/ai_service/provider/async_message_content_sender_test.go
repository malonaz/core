package provider

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type recordingStream struct {
	ctx       context.Context
	mu        sync.Mutex
	responses []*aiservicepb.StreamGenerateMessageResponse
}

func (s *recordingStream) Send(response *aiservicepb.StreamGenerateMessageResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, response)
	return nil
}

func (s *recordingStream) Context() context.Context { return s.ctx }

func (s *recordingStream) heartbeats() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, response := range s.responses {
		if response.GetHeartbeat() != nil {
			n++
		}
	}
	return n
}

func TestAsyncMessageContentSenderHeartbeat(t *testing.T) {
	ctx := context.Background()

	stream := &recordingStream{ctx: ctx}
	sender := newAsyncMessageContentSender(stream, 0, 10*time.Millisecond)
	sender.SendBlocks(ctx, &aipb.Block{Content: &aipb.Block_Text{Text: "hello"}})
	require.Eventually(t, func() bool { return stream.heartbeats() >= 2 }, time.Second, time.Millisecond)
	sender.Close()
	require.NoError(t, sender.Wait(ctx))
	require.Equal(t, "hello", stream.responses[0].GetBlock().GetText())
}
