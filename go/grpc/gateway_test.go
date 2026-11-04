package grpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
)

func TestForwardResponseOptionHTTPHeadersForwarder(t *testing.T) {
	testCases := []struct {
		name               string
		headerToValue      map[string]string
		expectedStatusCode int
	}{
		{
			name:               "no headers",
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "cache control only",
			headerToValue:      map[string]string{HTTPHeaderCacheControl: "private, max-age=60"},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "location only",
			headerToValue:      map[string]string{HTTPHeaderLocation: "https://example.com/file"},
			expectedStatusCode: http.StatusFound,
		},
		{
			name: "location and cache control",
			headerToValue: map[string]string{
				HTTPHeaderLocation:     "https://example.com/file",
				HTTPHeaderCacheControl: "private, max-age=60",
			},
			expectedStatusCode: http.StatusFound,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// The forwarder ranges over a map: repeat so every iteration order is exercised.
			for range 100 {
				responseRecorder := httptest.NewRecorder()
				for key, value := range testCase.headerToValue {
					responseRecorder.Header().Set(runtime.MetadataHeaderPrefix+key, value)
				}
				require.NoError(t, forwardResponseOptionHTTPHeadersForwarder(context.Background(), responseRecorder, nil))

				// Result reports the headers as they were when the status was written.
				response := responseRecorder.Result()
				require.Equal(t, testCase.expectedStatusCode, response.StatusCode)
				for key, value := range testCase.headerToValue {
					require.Equal(t, value, response.Header.Get(key))
				}
			}
		})
	}
}
