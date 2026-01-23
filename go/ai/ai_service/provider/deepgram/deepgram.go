package deepgram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/coder/websocket"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	defaultBaseURL       = "wss://api.deepgram.com"
	ModelFluxGeneralEN   = "flux-general-en"
	EncodingLinear16     = "linear16"
	EncodingLinear32     = "linear32"
	MessageTypeConnected = "Connected"
	MessageTypeTurnInfo  = "TurnInfo"
	MessageTypeError     = "Error"
	MessageTypeFinalize  = "Finalize"
	EventUpdate          = "Update"
	EventStartOfTurn     = "StartOfTurn"
	EventEagerEndOfTurn  = "EagerEndOfTurn"
	EventTurnResumed     = "TurnResumed"
	EventEndOfTurn       = "EndOfTurn"
)

type Client struct {
	apiKey       string
	baseURL      string
	modelService *provider.ModelService
}

func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return &Client{apiKey: apiKey, baseURL: defaultBaseURL, modelService: modelService}
}

func (c *Client) ProviderId() string          { return provider.Deepgram }
func (c *Client) Start(context.Context) error { return nil }
func (c *Client) Stop()                       {}

type ListenOptions struct {
	Model             string
	Encoding          string
	SampleRate        int
	EotTimeoutMs      int
	EagerEotThreshold float64
	EotThreshold      float64
}

type ListenConnection struct {
	conn *websocket.Conn
}

func (c *Client) Listen(ctx context.Context, opts *ListenOptions) (*ListenConnection, error) {
	params := url.Values{}
	params.Set("model", opts.Model)
	params.Set("encoding", opts.Encoding)
	params.Set("sample_rate", fmt.Sprintf("%d", opts.SampleRate))
	if opts.EotThreshold > 0 {
		params.Set("eot_threshold", fmt.Sprintf("%f", opts.EotThreshold))
	}
	if opts.EagerEotThreshold > 0 {
		params.Set("eager_eot_threshold", fmt.Sprintf("%f", opts.EagerEotThreshold))
	}
	if opts.EotTimeoutMs > 0 {
		params.Set("eot_timeout_ms", fmt.Sprintf("%d", opts.EotTimeoutMs))
	}

	wsURL := fmt.Sprintf("%s/v2/listen?%s", c.baseURL, params.Encode())
	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("token %s", c.apiKey))

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("dialing deepgram: %w", err)
	}
	return &ListenConnection{conn: conn}, nil
}

func (lc *ListenConnection) SendAudio(ctx context.Context, audio []byte) error {
	return lc.conn.Write(ctx, websocket.MessageBinary, audio)
}

func (lc *ListenConnection) Finalize(ctx context.Context) error {
	data, _ := json.Marshal(map[string]string{"type": MessageTypeFinalize})
	return lc.conn.Write(ctx, websocket.MessageText, data)
}

func (lc *ListenConnection) Close() error {
	return lc.conn.Close(websocket.StatusNormalClosure, "closing")
}

type ServerMessage struct {
	Type                string  `json:"type"`
	RequestID           string  `json:"request_id"`
	Event               string  `json:"event"`
	Transcript          string  `json:"transcript"`
	Code                string  `json:"code"`
	Description         string  `json:"description"`
	SequenceID          float64 `json:"sequence_id"`
	TurnIndex           int32   `json:"turn_index"`
	EndOfTurnConfidence float64 `json:"end_of_turn_confidence"`
}

func (lc *ListenConnection) ReceiveMessage(ctx context.Context) (*ServerMessage, error) {
	_, data, err := lc.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var msg ServerMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *ServerMessage) AsError() error {
	return fmt.Errorf("deepgram error [%s]: %s", m.Code, m.Description)
}

var _ provider.SpeechToTextStreamClient = (*Client)(nil)
