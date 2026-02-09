package ai

import (
	"fmt"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"google.golang.org/protobuf/proto"
)

type TextToTextAccumulator struct {
	Message           *aipb.Message
	StopReason        pb.TextToTextStopReason
	ModelUsage        *aipb.ModelUsage
	GenerationMetrics *aipb.GenerationMetrics
	blockIndexToBlock map[int64]*aipb.Block
}

func NewTextToTextAccumulator() *TextToTextAccumulator {
	return &TextToTextAccumulator{
		Message:           NewAssistantMessage(),
		blockIndexToBlock: map[int64]*aipb.Block{},
	}
}

func (a *TextToTextAccumulator) Add(response *pb.TextToTextStreamResponse) error {
	switch c := response.GetContent().(type) {
	case *pb.TextToTextStreamResponse_Block:
		block, ok := a.blockIndexToBlock[c.Block.Index]
		if !ok {
			block = &aipb.Block{Index: c.Block.Index}
			a.blockIndexToBlock[c.Block.Index] = block
			a.Message.Blocks = append(a.Message.Blocks, block)
		}
		if c.Block.Signature != "" {
			block.Signature = c.Block.Signature
		}
		if c.Block.ExtraFields != nil {
			block.ExtraFields = c.Block.ExtraFields
		}
		switch content := c.Block.Content.(type) {
		case *aipb.Block_Text:
			if block.Content == nil {
				block.Content = &aipb.Block_Text{}
			}
			existing, ok := block.Content.(*aipb.Block_Text)
			if !ok {
				return fmt.Errorf("block %d: received text content but block has type %T", c.Block.Index, block.Content)
			}
			existing.Text += content.Text
		case *aipb.Block_Thought:
			if block.Content == nil {
				block.Content = &aipb.Block_Thought{}
			}
			existing, ok := block.Content.(*aipb.Block_Thought)
			if !ok {
				return fmt.Errorf("block %d: received thought content but block has type %T", c.Block.Index, block.Content)
			}
			existing.Thought += content.Thought
		case *aipb.Block_ToolCall:
			if block.Content != nil {
				if _, ok := block.Content.(*aipb.Block_ToolCall); !ok {
					return fmt.Errorf("block %d: received tool_call content but block has type %T", c.Block.Index, block.Content)
				}
			}
			block.Content = content
		case *aipb.Block_Image:
			if block.Content != nil {
				if _, ok := block.Content.(*aipb.Block_Image); !ok {
					return fmt.Errorf("block %d: received image content but block has type %T", c.Block.Index, block.Content)
				}
			}
			block.Content = content
		case *aipb.Block_PartialToolCall:
		}

	case *pb.TextToTextStreamResponse_StopReason:
		a.StopReason = c.StopReason

	case *pb.TextToTextStreamResponse_ModelUsage:
		if a.ModelUsage == nil {
			a.ModelUsage = &aipb.ModelUsage{}
		}
		proto.Merge(a.ModelUsage, c.ModelUsage)

	case *pb.TextToTextStreamResponse_GenerationMetrics:
		if a.GenerationMetrics == nil {
			a.GenerationMetrics = &aipb.GenerationMetrics{}
		}
		proto.Merge(a.GenerationMetrics, c.GenerationMetrics)
		a.GenerationMetrics.Ttfb = nil
	}
	return nil
}

func (a *TextToTextAccumulator) Response() *pb.TextToTextResponse {
	return &pb.TextToTextResponse{
		Message:           a.Message,
		StopReason:        a.StopReason,
		ModelUsage:        a.ModelUsage,
		GenerationMetrics: a.GenerationMetrics,
	}
}
