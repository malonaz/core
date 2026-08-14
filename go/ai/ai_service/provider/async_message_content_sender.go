package provider

import (
	"context"
	"sync"
	"sync/atomic"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type AsyncMessageContentSender struct {
	srv  aiservicepb.AiService_StreamGenerateMessageServer
	ch   chan *aiservicepb.StreamGenerateMessageResponse
	done chan struct{}

	err  atomic.Value
	once sync.Once
}

func NewAsyncMessageContentSender(srv aiservicepb.AiService_StreamGenerateMessageServer, bufferSize int) *AsyncMessageContentSender {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	s := &AsyncMessageContentSender{
		srv:  srv,
		ch:   make(chan *aiservicepb.StreamGenerateMessageResponse, bufferSize),
		done: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *AsyncMessageContentSender) run() {
	defer close(s.done)
	ctx := s.srv.Context()
	for {
		select {
		case <-ctx.Done():
			s.err.Store(ctx.Err())
			return
		case resp, ok := <-s.ch:
			if !ok {
				return
			}
			if err := s.srv.Send(resp); err != nil {
				s.err.Store(err)
				return
			}
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
