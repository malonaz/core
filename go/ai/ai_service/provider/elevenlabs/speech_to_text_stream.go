package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/grpc"
)

const (
	sttWebSocketBaseURL                  = "wss://api.elevenlabs.io/v1/speech-to-text/realtime"
	msgTypeSessionStarted                = "session_started"
	msgTypePartialTranscript             = "partial_transcript"
	msgTypeCommittedTranscript           = "committed_transcript"
	msgTypeCommittedTranscriptTimestamps = "committed_transcript_with_timestamps"
	msgTypeInputError                    = "input_error"
)

type sttMessage struct {
	MessageType string `json:"message_type"`
	Text        string `json:"text,omitempty"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
}

type sttInputChunk struct {
	MessageType string `json:"message_type"`
	AudioBase64 string `json:"audio_base_64"`
	Commit      bool   `json:"commit"`
	SampleRate  int    `json:"sample_rate"`
}

func (c *Client) SpeechToTextStream(srv aiservicepb.AiService_SpeechToTextStreamServer) error {
	ctx := srv.Context()

	event, err := srv.Recv()
	if err != nil {
		return grpc.Errorf(codes.Internal, "receiving configuration event: %v", err).Err()
	}
	configuration := event.GetConfiguration()
	if configuration == nil {
		return grpc.Errorf(codes.FailedPrecondition, "first message must be configuration").Err()
	}

	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	audioFormat := configuration.AudioFormat
	if audioFormat.BitsPerSample != 16 {
		return grpc.Errorf(codes.InvalidArgument, "only 16-bit PCM audio supported").Err()
	}

	vadConfiguration := configuration.GetVad()
	if vadConfiguration == nil {
		return grpc.Errorf(codes.InvalidArgument, "only supports vad commit strategy").Err()
	}

	params := url.Values{}
	params.Set("model_id", model.ProviderModelId)
	params.Set("audio_format", fmt.Sprintf("pcm_%d", audioFormat.SampleRate))
	if v := configuration.GetLanguageCode(); v != "" {
		params.Set("language_code", v)
	}
	params.Set("commit_strategy", "vad")
	if d := vadConfiguration.GetSilenceThreshold(); d != nil {
		params.Set("vad_silence_threshold_secs", fmt.Sprintf("%.2f", d.AsDuration().Seconds()))
	}
	if v := vadConfiguration.GetVadThreshold(); v != 0 {
		params.Set("vad_threshold", fmt.Sprintf("%.2f", v))
	}
	if d := vadConfiguration.GetMinSpeechDuration(); d != nil {
		params.Set("min_speech_duration_ms", fmt.Sprintf("%d", d.AsDuration().Milliseconds()))
	}
	if d := vadConfiguration.GetMinSilenceDuration(); d != nil {
		params.Set("min_silence_duration_ms", fmt.Sprintf("%d", d.AsDuration().Milliseconds()))
	}

	wsURL := fmt.Sprintf("%s?%s", sttWebSocketBaseURL, params.Encode())
	header := http.Header{}
	header.Set("xi-api-key", c.apiKey)

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return grpc.Errorf(codes.Internal, "connecting to elevenlabs: %v", err).Err()
	}
	defer conn.Close(websocket.StatusNormalClosure, "closing")

	msg, err := c.receiveSTTMessage(ctx, conn)
	if err != nil {
		return grpc.Errorf(codes.Internal, "receiving session_started: %v", err).Err()
	}
	if msg.MessageType != msgTypeSessionStarted {
		return grpc.Errorf(codes.Internal, "expected session_started, got %s", msg.MessageType).Err()
	}

	errChan := make(chan error, 2)
	go func() { errChan <- c.recvSTTAudio(ctx, srv, conn, int(audioFormat.SampleRate)) }()
	go func() { errChan <- c.sendSTTEvents(ctx, srv, conn) }()
	return <-errChan
}

func (c *Client) recvSTTAudio(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *websocket.Conn, sampleRate int) error {
	for {
		req, err := srv.Recv()
		if err == io.EOF {
			return c.sendSTTChunk(ctx, conn, nil, true, sampleRate)
		}
		if err != nil {
			return err
		}
		if chunk := req.GetAudioChunk(); chunk != nil {
			if err := c.sendSTTChunk(ctx, conn, chunk.Data, false, sampleRate); err != nil {
				return err
			}
		}
	}
}

func (c *Client) sendSTTChunk(ctx context.Context, conn *websocket.Conn, data []byte, commit bool, sampleRate int) error {
	msg := sttInputChunk{
		MessageType: "input_audio_chunk",
		AudioBase64: base64.StdEncoding.EncodeToString(data),
		Commit:      commit,
		SampleRate:  sampleRate,
	}
	encoded, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, encoded)
}

func (c *Client) sendSTTEvents(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *websocket.Conn) error {
	var turnIndex int32
	for {
		msg, err := c.receiveSTTMessage(ctx, conn)
		if err != nil {
			return err
		}
		switch msg.MessageType {
		case msgTypeInputError:
			return grpc.Errorf(codes.Internal, "elevenlabs error [%s]: %s", msg.Code, msg.Message).Err()
		case msgTypePartialTranscript:
			if err := srv.Send(&aiservicepb.SpeechToTextStreamResponse{
				Content: &aiservicepb.SpeechToTextStreamResponse_TurnUpdate{
					TurnUpdate: &aiservicepb.SpeechToTextStreamTurnEvent{
						TurnIndex:  turnIndex,
						Transcript: msg.Text,
					},
				},
			}); err != nil {
				return err
			}
		case msgTypeCommittedTranscript, msgTypeCommittedTranscriptTimestamps:
			if msg.Text == "" {
				continue
			}
			if err := srv.Send(&aiservicepb.SpeechToTextStreamResponse{
				Content: &aiservicepb.SpeechToTextStreamResponse_TurnEnd{
					TurnEnd: &aiservicepb.SpeechToTextStreamTurnEvent{
						TurnIndex:  turnIndex,
						Transcript: msg.Text,
					},
				},
			}); err != nil {
				return err
			}
			turnIndex++
		}
	}
}

func (c *Client) receiveSTTMessage(ctx context.Context, conn *websocket.Conn) (*sttMessage, error) {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var msg sttMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
