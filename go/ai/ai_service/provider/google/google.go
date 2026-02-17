package google

import (
	"context"
	"fmt"

	"cloud.google.com/go/auth/credentials"
	"google.golang.org/genai"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

type Opts struct {
	APIKey         string `long:"api-key"     env:"API_KEY" description:"API key"`
	Project        string `long:"cloud-project" env:"CLOUD_PROJECT" description:"Google Cloud Project"`
	Location       string `long:"cloud-location" env:"CLOUD_LOCATION" description:"Google Cloud Location"`
	ServiceAccount string `long:"cloud-service-account" env:"CLOUD_SERVICE_ACCOUNT" description:"Google Cloud Service Account"`
}

func (o *Opts) Valid() bool {
	if o == nil {
		return false
	}
	return o.APIKey != "" || o.ServiceAccount != ""
}

type Client struct {
	config         *genai.ClientConfig
	serviceAccount string
	client         *genai.Client
	modelService   *provider.ModelService
}

func NewClient(opts *Opts, modelService *provider.ModelService) *Client {
	if opts.APIKey != "" {
		return &Client{
			config: &genai.ClientConfig{
				APIKey: opts.APIKey,
			},
			modelService: modelService,
		}
	}
	return &Client{
		config: &genai.ClientConfig{
			Project:  opts.Project,
			Location: opts.Location,
			Backend:  genai.BackendVertexAI,
		},
		serviceAccount: opts.ServiceAccount,
		modelService:   modelService,
	}
}

func (c *Client) Name() string {
	if c.config.Backend == genai.BackendVertexAI {
		return "google (Vertex)"
	}
	return c.ProviderId()
}

func (c *Client) Start(ctx context.Context) error {
	if c.serviceAccount != "" {
		creds, err := credentials.DetectDefault(&credentials.DetectOptions{
			CredentialsJSON: []byte(c.serviceAccount),
			Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
		})
		if err != nil {
			return fmt.Errorf("parsing service account: %w", err)
		}
		c.config.Credentials = creds
	}
	client, err := genai.NewClient(ctx, c.config)
	if err != nil {
		return fmt.Errorf("creating genai client: %w", err)
	}
	c.client = client
	return nil
}

func (c *Client) ProviderId() string { return provider.Google }

func (c *Client) Stop() {}

var (
	_ provider.TextToTextClient = (*Client)(nil)
)
