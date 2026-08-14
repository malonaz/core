package ai_service

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/grpc/status"
)

// textToTextDefaultUserRn owns the chats implicitly created by the legacy
// stateless API, which has no notion of an organization or user.
var textToTextDefaultUserRn = &aipb.UserResourceName{
	Organization: "unknown",
	User:         "unknown",
}

// TextToText is the legacy stateless flavor of GenerateMessage.
//
// Deprecated: use GenerateMessage.
func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
	textToTextStreamRequest := &pb.TextToTextStreamRequest{
		Parent:        request.GetParent(),
		Model:         request.GetModel(),
		Messages:      request.GetMessages(),
		Tools:         request.GetTools(),
		ToolSets:      request.GetToolSets(),
		Configuration: request.GetConfiguration(),
		Labels:        request.GetLabels(),
	}
	generateMessageRequest, err := textToTextStreamRequestToGenerateMessageRequest(textToTextStreamRequest)
	if err != nil {
		return nil, err
	}

	generateMessageResponse, err := s.GenerateMessage(ctx, generateMessageRequest)
	if err != nil {
		return nil, err
	}

	return &pb.TextToTextResponse{
		Message:           generateMessageResponse.GetGeneratedMessage(),
		StopReason:        stopReasonToTextToTextStopReason(generateMessageResponse.GetStopReason()),
		ModelUsage:        generateMessageResponse.GetModelUsage(),
		GenerationMetrics: generateMessageResponse.GetGenerationMetrics(),
	}, nil
}

// TextToTextStream is the legacy stateless flavor of StreamGenerateMessage.
//
// Deprecated: use StreamGenerateMessage.
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.AiService_TextToTextStreamServer) error {
	generateMessageRequest, err := textToTextStreamRequestToGenerateMessageRequest(request)
	if err != nil {
		return err
	}

	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.GenerateMessageRequest,
		pb.StreamGenerateMessageResponse,
		pb.AiService_StreamGenerateMessageServer,
	](s.StreamGenerateMessage)

	stream, err := serverStreamClient(srv.Context(), generateMessageRequest)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// The legacy stream has no `generated_message` event: the caller
		// assembles the message from the block deltas it already received.
		textToTextStreamResponse := streamGenerateMessageResponseToTextToTextStreamResponse(response)
		if textToTextStreamResponse == nil {
			continue
		}
		if err := srv.Send(textToTextStreamResponse); err != nil {
			return err
		}
	}
}

// textToTextStreamRequestToGenerateMessageRequest adapts a stateless legacy
// request onto the chat-backed API. The legacy API carries the whole
// conversation in every request, so each call targets a fresh chat: the
// request's messages become that chat's history and nothing is inherited.
func textToTextStreamRequestToGenerateMessageRequest(request *pb.TextToTextStreamRequest) (*pb.GenerateMessageRequest, error) {
	generatedChatRn := textToTextDefaultUserRn.ChatResourceName(aip.NewSystemGeneratedBase32ResourceID())
	chatRn := &generatedChatRn
	if request.GetParent() != "" {
		chatRn = &aipb.ChatResourceName{}
		if err := chatRn.UnmarshalString(request.GetParent()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unmarshaling parent: %v", err).Err()
		}
	}

	// Soft-deleted messages were dropped client-side by the legacy API.
	messages := make([]*aipb.Message, 0, len(request.GetMessages()))
	for _, message := range request.GetMessages() {
		if message.GetDeleteTime() != nil {
			continue
		}
		messages = append(messages, message)
	}

	return &pb.GenerateMessageRequest{
		Parent:        chatRn.String(),
		Model:         request.GetModel(),
		Messages:      messages,
		Tools:         request.GetTools(),
		ToolSets:      request.GetToolSets(),
		Configuration: textToTextConfigurationToMessageGenerationConfiguration(request.GetConfiguration()),
		Labels:        request.GetLabels(),
	}, nil
}

func textToTextConfigurationToMessageGenerationConfiguration(configuration *pb.TextToTextConfiguration) *pb.MessageGenerationConfiguration {
	if configuration == nil {
		return nil
	}
	messageGenerationConfiguration := &pb.MessageGenerationConfiguration{
		MaxTokens:              configuration.GetMaxTokens(),
		Temperature:            configuration.GetTemperature(),
		ToolChoice:             configuration.GetToolChoice(),
		ReasoningEffort:        configuration.GetReasoningEffort(),
		StreamPartialToolCalls: configuration.GetStreamPartialToolCalls(),
	}
	if imageConfig := configuration.GetImageConfig(); imageConfig != nil {
		messageGenerationConfiguration.ImageConfiguration = &pb.ImageGenerationConfiguration{
			AspectRatio: imageConfig.GetAspectRatio(),
			ImageSize:   imageConfig.GetImageSize(),
		}
	}
	return messageGenerationConfiguration
}

// streamGenerateMessageResponseToTextToTextStreamResponse returns nil for
// events that have no legacy equivalent.
func streamGenerateMessageResponseToTextToTextStreamResponse(response *pb.StreamGenerateMessageResponse) *pb.TextToTextStreamResponse {
	switch content := response.GetContent().(type) {
	case *pb.StreamGenerateMessageResponse_Block:
		return &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_Block{Block: content.Block},
		}
	case *pb.StreamGenerateMessageResponse_StopReason:
		return &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_StopReason{
				StopReason: stopReasonToTextToTextStopReason(content.StopReason),
			},
		}
	case *pb.StreamGenerateMessageResponse_ModelUsage:
		return &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_ModelUsage{ModelUsage: content.ModelUsage},
		}
	case *pb.StreamGenerateMessageResponse_GenerationMetrics:
		return &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_GenerationMetrics{GenerationMetrics: content.GenerationMetrics},
		}
	}
	return nil
}

func stopReasonToTextToTextStopReason(stopReason pb.StopReason) pb.TextToTextStopReason {
	// Both enums declare the same reasons in the same order.
	return pb.TextToTextStopReason(stopReason)
}
