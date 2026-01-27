package provider

import (
	"context"
	"sync"
	"sync/atomic"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type AsyncTextToTextContentSender struct {
	srv  aiservicepb.AiService_TextToTextStreamServer
	ch   chan *aiservicepb.TextToTextStreamResponse
	done chan struct{}

	err  atomic.Value
	once sync.Once
}

func NewAsyncTextToTextContentSender(srv aiservicepb.AiService_TextToTextStreamServer, bufferSize int) *AsyncTextToTextContentSender {
	if bufferSize <= 0 {
		bufferSize = 64
	}
	s := &AsyncTextToTextContentSender{
		srv:  srv,
		ch:   make(chan *aiservicepb.TextToTextStreamResponse, bufferSize),
		done: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *AsyncTextToTextContentSender) run() {
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

func (s *AsyncTextToTextContentSender) Err() error {
	if v := s.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (s *AsyncTextToTextContentSender) Close() {
	s.once.Do(func() { close(s.ch) })
}

func (s *AsyncTextToTextContentSender) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.Err()
	}
}

func (s *AsyncTextToTextContentSender) enqueue(ctx context.Context, resp *aiservicepb.TextToTextStreamResponse) {
	if err := s.Err(); err != nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-s.srv.Context().Done():
	case s.ch <- resp:
	}
}

func (s *AsyncTextToTextContentSender) SendBlocks(ctx context.Context, blocks ...*aipb.Block) {
	for _, block := range blocks {
		s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
			Content: &aiservicepb.TextToTextStreamResponse_Block{Block: block},
		})
	}
}

func (s *AsyncTextToTextContentSender) SendStopReason(ctx context.Context, r aiservicepb.TextToTextStopReason) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_StopReason{StopReason: r},
	})
}

func (s *AsyncTextToTextContentSender) SendModelUsage(ctx context.Context, u *aipb.ModelUsage) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_ModelUsage{ModelUsage: u},
	})
}

func (s *AsyncTextToTextContentSender) SendGenerationMetrics(ctx context.Context, m *aipb.GenerationMetrics) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_GenerationMetrics{GenerationMetrics: m},
	})
}
