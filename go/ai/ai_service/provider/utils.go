package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
)

func NewModelName(provider, model string) string {
	return (&aipb.ModelRn{Provider: provider, Model: model}).String()
}

func parseModels(bytes []byte) (*aipb.ProviderModelConfig, error) {
	config := &aipb.ProviderModelConfig{}
	err := pbutil.JSONUnmarshalStrict(bytes, config)
	return config, err
}

// Download fetches a URL-sourced attachment for providers that only accept inline data.
func Download(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("downloading %s: %w", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloading %s: status %d", url, response.StatusCode)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", url, err)
	}
	return data, nil
}
