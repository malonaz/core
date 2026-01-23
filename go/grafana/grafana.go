package grafana

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/client/folders"
	"github.com/grafana/grafana-openapi-client-go/models"
)

// Opts for a grafana client.
type Opts struct {
	IAPBearerToken string `long:"iap-bearer-token" env:"IAP_BEARER_TOKEN"`
	APIKey         string `long:"api-key" env:"API_KEY" description:" API key" required:"true"`
	APIURL         string `long:"api-url" env:"API_URL" description:" API url" required:"true"`
	HTTPScheme     string `long:"http-scheme" env:"HTTP_SCHEME" default:"http"`
}

// An alias for the client.
type Client struct {
	*client.GrafanaHTTPAPI
}

// NewClient instantiates and returns a new Grafana client.
func NewClient(opts *Opts) (*Client, error) {
	headers := map[string]string{
		"x-disable-provenance": "",
	}
	if opts.IAPBearerToken != "" {
		headers["Proxy-Authorization"] = "Bearer " + opts.IAPBearerToken
	}

	cfg := &client.TransportConfig{
		// Host is the doman name or IP address of the host that serves the API.
		Host: opts.APIURL,
		// BasePath is the URL prefix for all API paths, relative to the host root.
		BasePath: "/api",
		// Schemes are the transfer protocols used by the API (http or https).
		Schemes: []string{opts.HTTPScheme},
		// APIKey is an optional API key or service account token.
		APIKey: opts.APIKey,
		// TLSConfig provides an optional configuration for a TLS client
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		// NumRetries contains the optional number of attempted retries
		NumRetries: 3,
		// RetryTimeout sets an optional time to wait before retrying a request
		RetryTimeout: 1 * time.Second,
		// RetryStatusCodes contains the optional list of status codes to retry
		// Use "x" as a wildcard for a single digit (default: [429, 5xx])
		RetryStatusCodes: []string{"5xx"},
		// HTTPHeaders contains an optional map of HTTP headers to add to each request
		HTTPHeaders: headers,
	}
	client := client.NewHTTPClientWithConfig(strfmt.Default, cfg)
	return &Client{client}, nil
}

func (c *Client) CreateFolderIfNotExist(ctx context.Context, title string) (*models.Folder, error) {
	folders, err := c.Folders.GetFolders(&folders.GetFoldersParams{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("getting folders: %w", err)
	}
	folderUID := ""
	for _, folder := range folders.Payload {
		if folder.Title == title {
			folderUID = folder.UID
			break
		}
	}
	if folderUID != "" {
		// Fetch it.
		response, err := c.Folders.GetFolderByUID(folderUID)
		if err != nil {
			return nil, fmt.Errorf("getting existint folder: %w", err)
		}
		return response.Payload, nil
	}

	// We must create the folder.
	response, err := c.Folders.CreateFolder(&models.CreateFolderCommand{Title: title})
	if err != nil {
		return nil, fmt.Errorf("creating folder: %w", err)
	}
	return response.Payload, nil
}
