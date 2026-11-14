package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// HeartbeatInterval is well under the 60s idle timeout of AWS load balancers.
const HeartbeatInterval = 15 * time.Second

// MessageStream is the minimal stream surface the sender pumps into: both the
// gRPC server stream and the service-layer interception wrapper satisfy it.
type MessageStream interface {
	Send(*aiservicepb.StreamGenerateMessageResponse) error
	Context() context.Context
}

type AsyncMessageContentSender struct {
	srv               MessageStream
	ch                chan *aiservicepb.StreamGenerateMessageResponse
	done              chan struct{}
	heartbeatInterval time.Duration

	err  atomic.Value
	once sync.Once
}

func NewAsyncMessageContentSender(srv MessageStream, bufferSize int) *AsyncMessageContentSender {
	return newAsyncMessageContentSender(srv, bufferSize, HeartbeatInterval)
}

func newAsyncMessageContentSender(srv MessageStream, bufferSize int, heartbeatInterval time.Duration) *AsyncMessageContentSender {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	s := &AsyncMessageContentSender{
		srv:               srv,
		ch:                make(chan *aiservicepb.StreamGenerateMessageResponse, bufferSize),
		done:              make(chan struct{}),
		heartbeatInterval: heartbeatInterval,
	}
	go s.run()
	return s
}

// run is the stream's only sender, so heartbeats never race a provider event.
func (s *AsyncMessageContentSender) run() {
	defer close(s.done)
	ctx := s.srv.Context()
	heartbeat := time.NewTicker(s.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		var resp *aiservicepb.StreamGenerateMessageResponse
		select {
		case <-ctx.Done():
			s.err.Store(ctx.Err())
			return
		case <-heartbeat.C:
			resp = &aiservicepb.StreamGenerateMessageResponse{
				Content: &aiservicepb.StreamGenerateMessageResponse_Heartbeat{Heartbeat: &emptypb.Empty{}},
			}
		case event, ok := <-s.ch:
			if !ok {
				return
			}
			resp = event
		}
		if err := s.srv.Send(resp); err != nil {
			s.err.Store(err)
			return
		}
	}
}

func (s *AsyncMessageContentSender) Err() error {
	if v := s.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (s *AsyncMessageContentSender) Close() {
	s.once.Do(func() { close(s.ch) })
}

func (s *AsyncMessageContentSender) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.Err()
	}
}

func (s *AsyncMessageContentSender) enqueue(ctx context.Context, resp *aiservicepb.StreamGenerateMessageResponse) {
	if err := s.Err(); err != nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-s.srv.Context().Done():
	case s.ch <- resp:
	}
}

func (s *AsyncMessageContentSender) SendBlocks(ctx context.Context, blocks ...*aipb.Block) {
	for _, block := range blocks {
		s.enqueue(ctx, &aiservicepb.StreamGenerateMessageResponse{
			Content: &aiservicepb.StreamGenerateMessageResponse_Block{Block: block},
		})
	}
}

func (s *AsyncMessageContentSender) SendStopReason(ctx context.Context, r aiservicepb.StopReason) {
	s.enqueue(ctx, &aiservicepb.StreamGenerateMessageResponse{
		Content: &aiservicepb.StreamGenerateMessageResponse_StopReason{StopReason: r},
	})
}

func (s *AsyncMessageContentSender) SendModelUsage(ctx context.Context, u *aipb.ModelUsage) {
	s.enqueue(ctx, &aiservicepb.StreamGenerateMessageResponse{
		Content: &aiservicepb.StreamGenerateMessageResponse_ModelUsage{ModelUsage: u},
	})
}

func (s *AsyncMessageContentSender) SendGenerationMetrics(ctx context.Context, m *aipb.GenerationMetrics) {
	s.enqueue(ctx, &aiservicepb.StreamGenerateMessageResponse{
		Content: &aiservicepb.StreamGenerateMessageResponse_GenerationMetrics{GenerationMetrics: m},
	})
}
