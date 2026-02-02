package deepgram

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/grpc"
)

func (c *Client) SpeechToTextStream(srv aiservicepb.AiService_SpeechToTextStreamServer) error {
	ctx := srv.Context()

	// Grab the configuration event and validate it.
	event, err := srv.Recv()
	if err != nil {
		return grpc.Errorf(codes.Internal, "receiving configuration event: %v", err).Err()
	}
	configuration := event.GetConfiguration()
	if configuration == nil {
		return grpc.Errorf(codes.FailedPrecondition, "first message must be configuration", err).Err()
	}

	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}
	if model.ProviderModelId == "nova-3" {
		return c.speechToTextStreamNova(srv, configuration)
	}

	endOfTurnConfiguration := configuration.GetEndOfTurn()
	if endOfTurnConfiguration == nil {
		return grpc.Errorf(codes.InvalidArgument, "only supports end_of_turn commit strategy", err).Err()
	}

	if endOfTurnConfiguration.ConfidenceThreshold > 0 {
		if endOfTurnConfiguration.ConfidenceThreshold < 0.5 || endOfTurnConfiguration.ConfidenceThreshold > 0.9 {
			return grpc.Errorf(codes.InvalidArgument, "end_of_turn_confidence_threshold must be between 0.5 and 0.9").Err()
		}
	}
	if endOfTurnConfiguration.EagerConfidenceThreshold > 0 {
		if endOfTurnConfiguration.EagerConfidenceThreshold < 0.3 || endOfTurnConfiguration.EagerConfidenceThreshold > 0.9 {
			return grpc.Errorf(codes.InvalidArgument, "eager_end_of_turn_confidence_threshold must be between 0.3 and 0.9").Err()
		}
	}

	encoding, err := encodingFromFormat(configuration.AudioFormat)
	if err != nil {
		return grpc.Errorf(codes.FailedPrecondition, "invalid audio format: %v", err).Err()
	}

	var eotTimeoutMs int
	if d := endOfTurnConfiguration.GetTimeout(); d != nil {
		eotTimeoutMs = int(d.AsDuration().Milliseconds())
	}

	conn, err := c.Listen(ctx, &ListenOptions{
		Model:             model.ProviderModelId,
		Encoding:          encoding,
		SampleRate:        int(configuration.AudioFormat.SampleRate),
		EotThreshold:      endOfTurnConfiguration.ConfidenceThreshold,
		EagerEotThreshold: endOfTurnConfiguration.EagerConfidenceThreshold,
		EotTimeoutMs:      eotTimeoutMs,
	})
	if err != nil {
		return grpc.Errorf(codes.Internal, "connecting to deepgram: %v", err).Err()
	}
	defer conn.Close()

	errChan := make(chan error, 2)
	go func() { errChan <- c.recvAudio(ctx, srv, conn) }()
	go func() { errChan <- c.sendEvents(ctx, srv, conn) }()
	return <-errChan
}

func (c *Client) recvAudio(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *ListenConnection) error {
	for {
		req, err := srv.Recv()
		if err == io.EOF {
			return conn.Finalize(ctx)
		}
		if err != nil {
			return err
		}
		if chunk := req.GetAudioChunk(); chunk != nil {
			if err := conn.SendAudio(ctx, chunk.Data); err != nil {
				return err
			}
		}
	}
}

func (c *Client) sendEvents(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *ListenConnection) error {
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		switch msg.Type {
		case MessageTypeError:
			return msg.AsError()
		case MessageTypeConnected:
			continue
		case MessageTypeTurnInfo:
			if resp := turnInfoToResponse(msg); resp != nil {
				if err := srv.Send(resp); err != nil {
					return err
				}
			}
		}
	}
}

func turnInfoToResponse(msg *ServerMessage) *aiservicepb.SpeechToTextStreamResponse {
	switch msg.Event {
	case EventStartOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnStart{
				TurnStart: &aiservicepb.SpeechToTextStreamTurnEvent{
					TurnIndex:           msg.TurnIndex,
					Transcript:          msg.Transcript,
					EndOfTurnConfidence: msg.EndOfTurnConfidence,
				},
			},
		}

	case EventUpdate:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnUpdate{
				TurnUpdate: &aiservicepb.SpeechToTextStreamTurnEvent{
					TurnIndex:           msg.TurnIndex,
					Transcript:          msg.Transcript,
					EndOfTurnConfidence: msg.EndOfTurnConfidence,
				},
			},
		}

	case EventEagerEndOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnEagerEnd{
				TurnEagerEnd: &aiservicepb.SpeechToTextStreamTurnEvent{
					TurnIndex:           msg.TurnIndex,
					Transcript:          msg.Transcript,
					EndOfTurnConfidence: msg.EndOfTurnConfidence,
				},
			},
		}

	case EventTurnResumed:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnResumed{
				TurnResumed: &aiservicepb.SpeechToTextStreamTurnEvent{
					TurnIndex:           msg.TurnIndex,
					Transcript:          msg.Transcript,
					EndOfTurnConfidence: msg.EndOfTurnConfidence,
				},
			},
		}

	case EventEndOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnEnd{
				TurnEnd: &aiservicepb.SpeechToTextStreamTurnEvent{
					TurnIndex:           msg.TurnIndex,
					Transcript:          msg.Transcript,
					EndOfTurnConfidence: msg.EndOfTurnConfidence,
				},
			},
		}

	}
	return nil
}

func encodingFromFormat(f *audiopb.Format) (string, error) {
	switch f.BitsPerSample {
	case 16:
		return EncodingLinear16, nil
	case 32:
		return EncodingLinear32, nil
	default:
		return "", fmt.Errorf("unsupported bits per sample: %d", f.BitsPerSample)
	}
}
