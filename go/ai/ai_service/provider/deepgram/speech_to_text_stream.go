package deepgram

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/grpc/status"
)

func (c *Client) SpeechToTextStream(srv aiservicepb.AiService_SpeechToTextStreamServer) error {
	ctx := srv.Context()

	// First message must be the stream configuration.
	event, err := srv.Recv()
	if err != nil {
		return status.FromError(err, "receiving configuration event").Err()
	}
	configuration := event.GetConfiguration()
	if configuration == nil {
		return status.Errorf(codes.FailedPrecondition, "first message must be configuration").Err()
	}

	// Resolve the provider-specific model ID.
	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return status.FromError(err, "getting model").Err()
	}

	// Deepgram only supports end-of-turn based commit strategy.
	endOfTurnConfiguration := configuration.GetEndOfTurn()
	if endOfTurnConfiguration == nil {
		return status.Errorf(codes.InvalidArgument, "end_of_turn configuration is required").Err()
	}

	encoding, err := encodingFromFormat(configuration.AudioFormat)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid audio format: %v", err).Err()
	}

	// Convert protobuf duration to milliseconds for Deepgram's API.
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
		return status.FromError(err, "connecting to deepgram").Err()
	}
	defer conn.Close()

	// Configure language hints if provided, sent as a post-connect configuration message.
	if len(configuration.LanguageCodes) > 0 {
		configureOptions := &ConfigureOptions{
			LanguageHints: configuration.LanguageCodes,
		}
		if err := conn.Configure(ctx, configureOptions); err != nil {
			return status.FromError(err, "configuring language hints").Err()
		}
	}

	// Run audio ingestion and event forwarding concurrently.
	// Cancel propagation ensures the other goroutine unblocks when one exits.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, 2)
	go func() {
		err := c.recvAudio(ctx, srv, conn)
		errChan <- err
	}()
	go func() {
		err := c.sendEvents(ctx, srv, conn)
		errChan <- err
	}()

	if err := <-errChan; err != nil {
		return status.FromError(err, "streaming speech to text").Err()
	}
	return nil
}

// recvAudio reads audio chunks and reconfigurations from the gRPC stream and forwards them to Deepgram.
func (c *Client) recvAudio(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *ListenConnection) error {
	for {
		speechToTextStreamRequest, err := srv.Recv()
		if err == io.EOF {
			// Client closed the stream; finalize to flush any pending transcription.
			return conn.Finalize(ctx)
		}
		if err != nil {
			return err
		}
		switch content := speechToTextStreamRequest.Content.(type) {
		case *aiservicepb.SpeechToTextStreamRequest_AudioChunk:
			if err := conn.SendAudio(ctx, content.AudioChunk.Data); err != nil {
				return err
			}
		case *aiservicepb.SpeechToTextStreamRequest_Configuration:
			if err := c.applyReconfiguration(ctx, conn, content.Configuration); err != nil {
				return err
			}
		}
	}
}

// applyReconfiguration translates a gRPC reconfiguration into a Deepgram Configure control message.
// Only set fields are forwarded; omitted fields remain unchanged on the Deepgram side.
func (c *Client) applyReconfiguration(ctx context.Context, conn *ListenConnection, reconfiguration *aiservicepb.SpeechToTextStreamConfiguration) error {
	configureOptions := &ConfigureOptions{
		LanguageHints: reconfiguration.LanguageCodes,
	}

	// Forward end-of-turn threshold updates if provided.
	if eot := reconfiguration.GetEndOfTurn(); eot != nil {
		configureOptions.Thresholds = &ConfigThresholds{}
		if eot.ConfidenceThreshold > 0 {
			configureOptions.Thresholds.EotThreshold = &eot.ConfidenceThreshold
		}
		if eot.EagerConfidenceThreshold > 0 {
			configureOptions.Thresholds.EagerEotThreshold = &eot.EagerConfidenceThreshold
		}
		if d := eot.GetTimeout(); d != nil {
			eotTimeoutMs := int(d.AsDuration().Milliseconds())
			configureOptions.Thresholds.EotTimeoutMs = &eotTimeoutMs
		}
	}

	return conn.Configure(ctx, configureOptions)
}

// sendEvents reads turn events from Deepgram and forwards them as gRPC responses.
func (c *Client) sendEvents(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *ListenConnection) error {
	for {
		message, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		switch message.Type {
		case MessageTypeError:
			return message.AsError()
		case MessageTypeConnected:
			continue
		case MessageTypeTurnInfo:
			speechToTextStreamResponse := turnInfoToResponse(message)
			if speechToTextStreamResponse == nil {
				continue
			}
			if err := srv.Send(speechToTextStreamResponse); err != nil {
				return err
			}
		}
	}
}

// turnInfoToResponse maps a Deepgram TurnInfo message to the corresponding gRPC response.
// Returns nil for unrecognized events.
func turnInfoToResponse(message *ServerMessage) *aiservicepb.SpeechToTextStreamResponse {
	// Deepgram returns detected and hinted languages directly on each TurnInfo.
	turnEvent := &aiservicepb.SpeechToTextStreamTurnEvent{
		TurnIndex:             message.TurnIndex,
		Transcript:            message.Transcript,
		EndOfTurnConfidence:   message.EndOfTurnConfidence,
		DetectedLanguageCodes: message.Languages,
		HintedLanguageCodes:   message.LanguagesHinted,
	}

	switch message.Event {
	case EventStartOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnStart{TurnStart: turnEvent},
		}
	case EventUpdate:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnUpdate{TurnUpdate: turnEvent},
		}
	case EventEagerEndOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnEagerEnd{TurnEagerEnd: turnEvent},
		}
	case EventTurnResumed:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnResumed{TurnResumed: turnEvent},
		}
	case EventEndOfTurn:
		return &aiservicepb.SpeechToTextStreamResponse{
			Content: &aiservicepb.SpeechToTextStreamResponse_TurnEnd{TurnEnd: turnEvent},
		}
	}
	return nil
}

func encodingFromFormat(format *audiopb.Format) (string, error) {
	switch format.BitsPerSample {
	case 16:
		return EncodingLinear16, nil
	case 32:
		return EncodingLinear32, nil
	default:
		return "", fmt.Errorf("unsupported bits per sample: %d", format.BitsPerSample)
	}
}
