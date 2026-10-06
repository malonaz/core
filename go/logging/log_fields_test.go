package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextHandlerEmitsLogFields(t *testing.T) {
	var buffer bytes.Buffer
	handler := NewContextHandler(slog.NewJSONHandler(&buffer, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}), extractLogFields)
	logger := slog.New(handler)

	ctx := WithLogFieldsTag(context.Background())
	InjectLogFields(ctx, "routine", "r1")
	logger.ErrorContext(ctx, "failed", "error", "boom")
	logger.Error("untagged")

	type line struct {
		Level   string `json:"level"`
		Msg     string `json:"msg"`
		Error   string `json:"error,omitempty"`
		Routine string `json:"routine,omitempty"`
	}
	var got []line
	decoder := json.NewDecoder(&buffer)
	for decoder.More() {
		var l line
		require.NoError(t, decoder.Decode(&l))
		got = append(got, l)
	}
	require.Equal(t, []line{
		{Level: "ERROR", Msg: "failed", Error: "boom", Routine: "r1"},
		{Level: "ERROR", Msg: "untagged"},
	}, got)
}
