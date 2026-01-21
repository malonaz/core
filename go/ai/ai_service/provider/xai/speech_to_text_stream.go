package xai

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/grpc"
)

const websocketEndpoint = "wss://api.x.ai/v1/realtime/audio/transcriptions"

type websocketConfigMessage struct {
	Type string              `json:"type"`
	Data websocketConfigData `json:"data"`
}

type websocketConfigData struct {
	Encoding             string `json:"encoding"`
	SampleRateHertz      int    `json:"sample_rate_hertz"`
	EnableInterimResults bool   `json:"enable_interim_results"`
}

type websocketAudioMessage struct {
	Type string             `json:"type"`
	Data websocketAudioData `json:"data"`
}

type websocketAudioData struct {
	Audio string `json:"audio"`
}

type websocketResponse struct {
	Data struct {
		Type string `json:"type"`
		Data struct {
			EventType  string `json:"event_type,omitempty"`
			Transcript string `json:"transcript,omitempty"`
			IsFinal    bool   `json:"is_final,omitempty"`
		} `json:"data"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

type turnState struct {
	index             int32
	inTurn            bool
	accumulatedFinals string
	currentInterim    string
}

func (t *turnState) startTurn() {
	t.inTurn = true
	t.index++
	t.accumulatedFinals = ""
	t.currentInterim = ""
}

func (t *turnState) addFinal(transcript string) {
	t.accumulatedFinals += transcript
	t.currentInterim = ""
}

func (t *turnState) setInterim(transcript string) string {
	t.currentInterim = transcript
	return t.accumulatedFinals + t.currentInterim
}

func (t *turnState) endTurn() (string, bool) {
	if !t.inTurn {
		return "", false
	}
	transcript := t.accumulatedFinals
	t.inTurn = false
	t.accumulatedFinals = ""
	t.currentInterim = ""
	return transcript, true
}

func (c *Client) SpeechToTextStream(srv aiservicepb.AiService_SpeechToTextStreamServer) error {
	ctx := srv.Context()

	event, err := srv.Recv()
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "receiving first message: %v", err).Err()
	}
	configuration := event.GetConfiguration()
	if configuration == nil {
		return grpc.Errorf(codes.InvalidArgument, "first event must contain configuration").Err()
	}

	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	if _, err := c.modelService.GetModel(ctx, getModelRequest); err != nil {
		return err
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+c.apiKey)
	connection, _, err := websocket.DefaultDialer.DialContext(ctx, websocketEndpoint, header)
	if err != nil {
		return grpc.Errorf(codes.Unavailable, "connecting to xai websocket: %v", err).Err()
	}
	defer connection.Close()

	configMsg := websocketConfigMessage{
		Type: "config",
		Data: websocketConfigData{
			Encoding:             "linear16",
			SampleRateHertz:      int(configuration.AudioFormat.SampleRate),
			EnableInterimResults: true,
		},
	}
	if err := connection.WriteJSON(configMsg); err != nil {
		return grpc.Errorf(codes.Internal, "sending configuration: %v", err).Err()
	}

	errChan := make(chan error, 2)
	state := &turnState{}

	go func() {
		for {
			var response websocketResponse
			if err := connection.ReadJSON(&response); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					errChan <- nil
					return
				}
				errChan <- fmt.Errorf("reading websocket response: %w", err)
				return
			}

			if response.Error != "" {
				errChan <- grpc.Errorf(codes.Internal, "xai error: %s", response.Error).Err()
				return
			}

			switch response.Data.Type {
			case "voice_activity":
				switch response.Data.Data.EventType {
				case "start":
					state.startTurn()
					srv.Send(&aiservicepb.SpeechToTextStreamResponse{
						Content: &aiservicepb.SpeechToTextStreamResponse_TurnStart{
							TurnStart: &aiservicepb.SpeechToTextStreamTurnEvent{TurnIndex: state.index},
						},
					})
				case "end":
					transcript, wasInTurn := state.endTurn()
					if wasInTurn {
						srv.Send(&aiservicepb.SpeechToTextStreamResponse{
							Content: &aiservicepb.SpeechToTextStreamResponse_TurnEnd{
								TurnEnd: &aiservicepb.SpeechToTextStreamTurnEvent{
									TurnIndex:  state.index,
									Transcript: transcript,
								},
							},
						})
					}
				}

			case "speech_recognized":
				transcript := response.Data.Data.Transcript
				if response.Data.Data.IsFinal {
					state.addFinal(transcript)
				} else if transcript != "" {
					srv.Send(&aiservicepb.SpeechToTextStreamResponse{
						Content: &aiservicepb.SpeechToTextStreamResponse_TurnUpdate{
							TurnUpdate: &aiservicepb.SpeechToTextStreamTurnEvent{
								TurnIndex:  state.index,
								Transcript: state.setInterim(transcript),
							},
						},
					})
				}
			}
		}
	}()

	go func() {
		for {
			message, err := srv.Recv()
			if err == io.EOF {
				connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				errChan <- nil
				return
			}
			if err != nil {
				errChan <- fmt.Errorf("receiving audio: %w", err)
				return
			}
			if chunk := message.GetAudioChunk(); chunk != nil {
				audioMsg := websocketAudioMessage{
					Type: "audio",
					Data: websocketAudioData{Audio: base64.StdEncoding.EncodeToString(chunk.Data)},
				}
				if err := connection.WriteJSON(audioMsg); err != nil {
					errChan <- fmt.Errorf("sending audio: %w", err)
					return
				}
			}
		}
	}()

	return <-errChan
}
