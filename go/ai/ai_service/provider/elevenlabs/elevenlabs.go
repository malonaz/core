package elevenlabs

import (
	"net/http"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	defaultBaseURL = "https://api.elevenlabs.io/v1"
)

type Client struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		client:  &http.Client{},
	}
}

// Implements the provider.Provider interface.
func (c *Client) Provider() aipb.Provider { return aipb.Provider_PROVIDER_ELEVENLABS }

// Verify interface implementation
var _ provider.TextToSpeechClient = (*Client)(nil)
