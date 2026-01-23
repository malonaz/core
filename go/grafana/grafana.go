package grafana

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/grafana/grafana-openapi-client-go/client"
	"github.com/grafana/grafana-openapi-client-go/client/folders"
	"github.com/grafana/grafana-openapi-client-go/models"
)

type Opts struct {
	IAPBearerToken string `long:"iap-bearer-token" env:"IAP_BEARER_TOKEN"`
	APIKey         string `long:"api-key" env:"API_KEY" description:" API key" required:"true"`
	APIURL         string `long:"api-url" env:"API_URL" description:" API url" required:"true"`
}

type Client struct {
	*client.GrafanaHTTPAPI
	baseURL string
}

func NewClient(opts *Opts) (*Client, error) {
	parsed, err := url.Parse(opts.APIURL)
	if err != nil {
		return nil, fmt.Errorf("parsing API URL: %w", err)
	}

	headers := map[string]string{
		"x-disable-provenance": "",
	}
	if opts.IAPBearerToken != "" {
		headers["Proxy-Authorization"] = "Bearer " + opts.IAPBearerToken
	}

	scheme := parsed.Scheme
	if scheme == "" {
		scheme = "https"
	}

	cfg := &client.TransportConfig{
		Host:             parsed.Host,
		BasePath:         "/api",
		Schemes:          []string{scheme},
		APIKey:           opts.APIKey,
		TLSConfig:        &tls.Config{InsecureSkipVerify: true},
		NumRetries:       3,
		RetryTimeout:     1 * time.Second,
		RetryStatusCodes: []string{"5xx"},
		HTTPHeaders:      headers,
	}
	c := client.NewHTTPClientWithConfig(strfmt.Default, cfg)
	return &Client{GrafanaHTTPAPI: c, baseURL: opts.APIURL}, nil
}

func (c *Client) BaseURL() string {
	return c.baseURL
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
		response, err := c.Folders.GetFolderByUID(folderUID)
		if err != nil {
			return nil, fmt.Errorf("getting existint folder: %w", err)
		}
		return response.Payload, nil
	}

	response, err := c.Folders.CreateFolder(&models.CreateFolderCommand{Title: title})
	if err != nil {
		return nil, fmt.Errorf("creating folder: %w", err)
	}
	return response.Payload, nil
}
