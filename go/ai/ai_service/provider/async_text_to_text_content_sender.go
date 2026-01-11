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

	err  atomic.Value // error
	once sync.Once
}

// bufferSize is the channel capacity; use a size that fits your burst profile.
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
		return
	case <-s.srv.Context().Done():
		return
	case s.ch <- resp:
		return
	}
}

// Typed helpers
func (s *AsyncTextToTextContentSender) SendGenerationMetrics(ctx context.Context, m *aipb.GenerationMetrics) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_GenerationMetrics{GenerationMetrics: m},
	})
}

func (s *AsyncTextToTextContentSender) SendContentChunk(ctx context.Context, chunk string) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_ContentChunk{ContentChunk: chunk},
	})
}

func (s *AsyncTextToTextContentSender) SendReasoningChunk(ctx context.Context, chunk string) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_ReasoningChunk{ReasoningChunk: chunk},
	})
}

func (s *AsyncTextToTextContentSender) SendToolCall(ctx context.Context, tcs ...*aipb.ToolCall) {
	for _, tc := range tcs {
		s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
			Content: &aiservicepb.TextToTextStreamResponse_ToolCall{ToolCall: tc},
		})
	}
}

func (s *AsyncTextToTextContentSender) SendPartialToolCall(ctx context.Context, tc *aipb.ToolCall) {
	s.enqueue(ctx, &aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_PartialToolCall{PartialToolCall: tc},
	})
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
