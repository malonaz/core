package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/grpc/codes"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
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

// PostJSON posts a JSON body to a provider and decodes its JSON response. A
// response other than 200 is returned as a status error carrying the
// provider's answer, coded so that callers can tell a request the provider
// rejected from a provider that is rate limiting or down.
func PostJSON(ctx context.Context, url string, headers map[string]string, requestBody, responseBody any) error {
	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		return status.Errorf(codes.Internal, "marshaling request: %v", err).Err()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBytes))
	if err != nil {
		return status.Errorf(codes.Internal, "building request: %v", err).Err()
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return status.FromError(err, "posting to %s", url).Err()
	}
	defer response.Body.Close()
	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return status.FromError(err, "reading response of %s", url).Err()
	}
	if response.StatusCode != http.StatusOK {
		return status.Errorf(codeFromHTTPStatus(response.StatusCode), "HTTP status %d: %s", response.StatusCode, responseBytes).Err()
	}
	if err := json.Unmarshal(responseBytes, responseBody); err != nil {
		return status.Errorf(codes.Internal, "unmarshaling response: %v", err).Err()
	}
	return nil
}

// codeFromHTTPStatus extends the canonical mapping to the statuses it leaves
// out (413, 422, 529...): a 4xx blames the request, a 5xx the provider.
func codeFromHTTPStatus(httpStatus int) codes.Code {
	code := grpc.CodeFromHTTPStatus(httpStatus)
	switch {
	case code != codes.Unknown:
		return code
	case httpStatus >= 500:
		return codes.Unavailable
	case httpStatus >= 400:
		return codes.InvalidArgument
	}
	return codes.Unknown
}
