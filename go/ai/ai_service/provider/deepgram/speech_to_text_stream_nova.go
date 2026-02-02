package deepgram

import (
	"context"
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
	NovaMessageTypeResults       = "Results"
	NovaMessageTypeSpeechStarted = "SpeechStarted"
	NovaMessageTypeUtteranceEnd  = "UtteranceEnd"
	NovaMessageTypeMetadata      = "Metadata"
	NovaMessageTypeError         = "Error"
)

type NovaListenOptions struct {
	Model          string
	Language       string
	Encoding       string
	SampleRate     int
	UtteranceEndMs int
	EndpointingMs  int
}

type NovaListenConnection struct {
	conn *websocket.Conn
}

type NovaServerMessage struct {
	Type         string          `json:"type"`
	RawChannel   json.RawMessage `json:"channel,omitempty"`
	Channel      *NovaChannel    `json:"-"`
	ChannelIndex []int           `json:"-"`
	Duration     float64         `json:"duration,omitempty"`
	Start        float64         `json:"start,omitempty"`
	IsFinal      bool            `json:"is_final,omitempty"`
	SpeechFinal  bool            `json:"speech_final,omitempty"`
	Timestamp    float64         `json:"timestamp,omitempty"`
	LastWordEnd  float64         `json:"last_word_end,omitempty"`
	Code         string          `json:"code,omitempty"`
	Description  string          `json:"description,omitempty"`
}

func (m *NovaServerMessage) UnmarshalJSON(data []byte) error {
	type Alias NovaServerMessage
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	if len(m.RawChannel) == 0 {
		return nil
	}
	if m.RawChannel[0] == '{' {
		var ch NovaChannel
		if err := json.Unmarshal(m.RawChannel, &ch); err != nil {
			return err
		}
		m.Channel = &ch
	} else if m.RawChannel[0] == '[' {
		if err := json.Unmarshal(m.RawChannel, &m.ChannelIndex); err != nil {
			return err
		}
	}
	return nil
}

type NovaChannel struct {
	Alternatives []NovaAlternative `json:"alternatives"`
}

type NovaAlternative struct {
	Transcript string  `json:"transcript"`
	Confidence float64 `json:"confidence"`
}

func (c *Client) ListenNova(ctx context.Context, opts *NovaListenOptions) (*NovaListenConnection, error) {
	params := url.Values{}
	params.Set("model", opts.Model)
	params.Set("encoding", opts.Encoding)
	params.Set("sample_rate", fmt.Sprintf("%d", opts.SampleRate))
	params.Set("channels", "1")
	params.Set("language", opts.Language)
	params.Set("interim_results", "true")
	params.Set("vad_events", "true")
	if opts.UtteranceEndMs > 0 {
		params.Set("utterance_end_ms", fmt.Sprintf("%d", opts.UtteranceEndMs))
	}
	if opts.EndpointingMs > 0 {
		params.Set("endpointing", fmt.Sprintf("%d", opts.EndpointingMs))
	}

	wsURL := fmt.Sprintf("%s/v1/listen?%s", c.baseURL, params.Encode())
	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("token %s", c.apiKey))

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dialing deepgram nova: %w", err)
	}
	return &NovaListenConnection{conn: conn}, nil
}

func (lc *NovaListenConnection) SendAudio(ctx context.Context, audio []byte) error {
	return lc.conn.Write(ctx, websocket.MessageBinary, audio)
}

func (lc *NovaListenConnection) Finalize(ctx context.Context) error {
	data, _ := json.Marshal(map[string]string{"type": MessageTypeFinalize})
	return lc.conn.Write(ctx, websocket.MessageText, data)
}

func (lc *NovaListenConnection) Close() error {
	return lc.conn.Close(websocket.StatusNormalClosure, "closing")
}

func (lc *NovaListenConnection) ReceiveMessage(ctx context.Context) (*NovaServerMessage, error) {
	_, data, err := lc.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var msg NovaServerMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *NovaServerMessage) AsError() error {
	return fmt.Errorf("deepgram nova error [%s]: %s", m.Code, m.Description)
}

func (c *Client) speechToTextStreamNova(
	srv aiservicepb.AiService_SpeechToTextStreamServer, configuration *aiservicepb.SpeechToTextStreamConfiguration,
) error {
	ctx := srv.Context()

	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	if configuration.LanguageCode == "" {
		configuration.LanguageCode = "multi"
	}
	vadConfiguration := configuration.GetVad()
	if vadConfiguration == nil {
		return grpc.Errorf(codes.InvalidArgument, "only supports vad commit strategy").Err()
	}

	encoding, err := encodingFromFormat(configuration.AudioFormat)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "invalid audio format: %v", err).Err()
	}

	var utteranceEndMs int
	if d := vadConfiguration.GetSilenceThreshold(); d != nil {
		utteranceEndMs = int(d.AsDuration().Milliseconds())
		if utteranceEndMs < 1000 {
			utteranceEndMs = 1000
		}
	}
	var endpointingMs int
	if d := vadConfiguration.GetMinSilenceDuration(); d != nil {
		endpointingMs = int(d.AsDuration().Milliseconds())
	}

	conn, err := c.ListenNova(ctx, &NovaListenOptions{
		Model:          model.ProviderModelId,
		Language:       configuration.LanguageCode,
		Encoding:       encoding,
		SampleRate:     int(configuration.AudioFormat.SampleRate),
		UtteranceEndMs: utteranceEndMs,
		EndpointingMs:  endpointingMs,
	})
	if err != nil {
		return grpc.Errorf(codes.Internal, "connecting to deepgram nova: %v", err).Err()
	}
	defer conn.Close()

	errChan := make(chan error, 2)
	go func() { errChan <- c.recvAudioNova(ctx, srv, conn) }()
	go func() { errChan <- c.sendEventsNova(ctx, srv, conn) }()
	return <-errChan
}

func (c *Client) recvAudioNova(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *NovaListenConnection) error {
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

func (c *Client) sendEventsNova(ctx context.Context, srv aiservicepb.AiService_SpeechToTextStreamServer, conn *NovaListenConnection) error {
	var turnIndex int32
	var turnStarted bool
	var lastTranscript string

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return err
		}

		var resp *aiservicepb.SpeechToTextStreamResponse
		switch msg.Type {
		case NovaMessageTypeError:
			return msg.AsError()
		case NovaMessageTypeMetadata, NovaMessageTypeSpeechStarted:
			continue
		case NovaMessageTypeUtteranceEnd:
			if !turnStarted {
				continue
			}
			resp = &aiservicepb.SpeechToTextStreamResponse{
				Content: &aiservicepb.SpeechToTextStreamResponse_TurnEnd{
					TurnEnd: &aiservicepb.SpeechToTextStreamTurnEvent{
						TurnIndex:  turnIndex,
						Transcript: lastTranscript,
					},
				},
			}
			turnIndex++
			turnStarted = false
			lastTranscript = ""
		case NovaMessageTypeResults:
			var transcript string
			if msg.Channel != nil && len(msg.Channel.Alternatives) > 0 {
				transcript = msg.Channel.Alternatives[0].Transcript
			}
			if transcript == "" {
				continue
			}
			transcript = concat(lastTranscript, transcript)
			if msg.IsFinal {
				lastTranscript = transcript
			}

			if !turnStarted {
				resp = &aiservicepb.SpeechToTextStreamResponse{
					Content: &aiservicepb.SpeechToTextStreamResponse_TurnStart{
						TurnStart: &aiservicepb.SpeechToTextStreamTurnEvent{
							TurnIndex:  turnIndex,
							Transcript: transcript,
						},
					},
				}
				turnStarted = true
			} else {
				resp = &aiservicepb.SpeechToTextStreamResponse{
					Content: &aiservicepb.SpeechToTextStreamResponse_TurnUpdate{
						TurnUpdate: &aiservicepb.SpeechToTextStreamTurnEvent{
							TurnIndex:  turnIndex,
							Transcript: transcript,
						},
					},
				}
			}
		}

		if resp != nil {
			if err := srv.Send(resp); err != nil {
				return err
			}
		}
	}
}

func concat(existing, new string) string {
	if existing == "" {
		return new
	}
	return existing + " " + new
}
