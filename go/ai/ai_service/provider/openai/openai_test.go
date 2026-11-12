package openai

import (
	"errors"
	"fmt"
	"testing"

	openai2 "github.com/openai/openai-go/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestAPIError(t *testing.T) {
	sdkError := &openai2.Error{StatusCode: 429}
	require.NoError(t, sdkError.UnmarshalJSON([]byte(`{"message": "Rate limit reached.", "type": "requests", "param": null, "code": "rate_limit_exceeded"}`)))

	// The provider's answer is in the message, and its status in the code.
	err := apiError(fmt.Errorf("wrapped: %w", sdkError), "reading stream")
	grpcrequire.Error(t, codes.ResourceExhausted, err)
	require.ErrorContains(t, err, "reading stream: HTTP status 429")
	require.ErrorContains(t, err, "Rate limit reached.")

	// Any other error keeps its text.
	err = apiError(errors.New("connection reset"), "reading stream")
	grpcrequire.Error(t, codes.Unknown, err)
	require.ErrorContains(t, err, "reading stream: connection reset")
}
