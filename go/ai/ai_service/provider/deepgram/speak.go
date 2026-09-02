package deepgram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/coder/websocket"
)

// Flux TTS (/v2/speak) message types.
const (
	SpeakMessageTypeSpeak             = "Speak"
	SpeakMessageTypeFlush             = "Flush"
	SpeakMessageTypeInterrupt         = "Interrupt"
	SpeakMessageTypeClose             = "Close"
	SpeakMessageTypeConnected         = "Connected"
	SpeakMessageTypeSpeechStarted     = "SpeechStarted"
	SpeakMessageTypeSpeechMetadata    = "SpeechMetadata"
	SpeakMessageTypeSpeechInterrupted = "SpeechInterrupted"
	SpeakMessageTypeFlushed           = "Flushed"
	SpeakMessageTypeSessionMetadata   = "SessionMetadata"
	SpeakMessageTypeWarning           = "Warning"
	SpeakMessageTypeError             = "Error"
)

// SpeakOptions holds the /v2/speak connection query parameters.
type SpeakOptions struct {
	// Flux model string, format flux-{voice}-{language} (e.g. flux-alexis-en).
	Model      string
	Encoding   string
	SampleRate int
	// Optional; valid values 0.85-1.15 in 0.05 increments.
	Speed string
	// Optional; valid values -2..2. Fixed for the connection.
	Expressivity string
}

// SpeakConnection wraps a websocket connection to Flux TTS (/v2/speak).
type SpeakConnection struct {
	conn *websocket.Conn
}

// Speak opens a websocket connection to the Flux TTS endpoint.
func (c *Client) Speak(ctx context.Context, opts *SpeakOptions) (*SpeakConnection, error) {
	params := url.Values{}
	params.Set("model", opts.Model)
	params.Set("encoding", opts.Encoding)
	if opts.SampleRate > 0 {
		params.Set("sample_rate", fmt.Sprintf("%d", opts.SampleRate))
	}
	if opts.Speed != "" {
		params.Set("speed", opts.Speed)
	}
	if opts.Expressivity != "" {
		params.Set("expressivity", opts.Expressivity)
	}

	wsURL := fmt.Sprintf("%s/v2/speak?%s", c.baseURL, params.Encode())
	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("token %s", c.apiKey))

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dialing deepgram speak: %w", err)
	}
	return &SpeakConnection{conn: conn}, nil
}

func (sc *SpeakConnection) sendControl(ctx context.Context, message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return sc.conn.Write(ctx, websocket.MessageText, data)
}

// SendText queues text for synthesis on the current turn.
func (sc *SpeakConnection) SendText(ctx context.Context, text string) error {
	return sc.sendControl(ctx, map[string]string{"type": SpeakMessageTypeSpeak, "text": text})
}

// Flush marks the end of the turn's text, forcing synthesis of any buffered input.
func (sc *SpeakConnection) Flush(ctx context.Context) error {
	return sc.sendControl(ctx, map[string]string{"type": SpeakMessageTypeFlush})
}

// Interrupt stops synthesis of the current turn; the server responds with
// SpeechInterrupted reporting what was spoken.
func (sc *SpeakConnection) Interrupt(ctx context.Context) error {
	return sc.sendControl(ctx, map[string]string{"type": SpeakMessageTypeInterrupt})
}

// SendClose asks the server to finish the session; the server responds with
// SessionMetadata before closing the websocket.
func (sc *SpeakConnection) SendClose(ctx context.Context) error {
	return sc.sendControl(ctx, map[string]string{"type": SpeakMessageTypeClose})
}

func (sc *SpeakConnection) Close() error {
	return sc.conn.Close(websocket.StatusNormalClosure, "closing")
}

// SpeakServerMessage is a union of all /v2/speak server messages.
// Audio holds raw audio bytes when the frame was binary (Type is empty).
type SpeakServerMessage struct {
	Audio []byte `json:"-"`

	Type        string `json:"type"`
	SpeechID    string `json:"speech_id"`
	Code        string `json:"code"`
	Description string `json:"description"`

	// SpeechMetadata fields.
	AudioDurationMs        int `json:"audio_duration_ms"`
	InputCharacterCount    int `json:"input_character_count"`
	BillableCharacterCount int `json:"billable_character_count"`

	// SpeechInterrupted fields.
	TextSpoken    string `json:"text_spoken"`
	TextRemaining string `json:"text_remaining"`

	// SessionMetadata fields.
	TotalAudioDurationMs        int `json:"total_audio_duration_ms"`
	TotalInputCharacterCount    int `json:"total_input_character_count"`
	TotalBillableCharacterCount int `json:"total_billable_character_count"`
}

// ReceiveMessage reads the next server frame; binary frames are returned with
// Audio set and Type empty.
func (sc *SpeakConnection) ReceiveMessage(ctx context.Context) (*SpeakServerMessage, error) {
	messageType, data, err := sc.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType == websocket.MessageBinary {
		return &SpeakServerMessage{Audio: data}, nil
	}
	var message SpeakServerMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

func (m *SpeakServerMessage) AsError() error {
	return fmt.Errorf("deepgram speak error [%s]: %s", m.Code, m.Description)
}
