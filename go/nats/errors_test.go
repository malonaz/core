package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestIsRejected(t *testing.T) {
	apiErr := func(code jetstream.ErrorCode) error {
		return fmt.Errorf("publishing message: %w", &jetstream.APIError{ErrorCode: code})
	}
	errs := map[string]error{
		"max payload":          fmt.Errorf("publishing message: %w", nats.ErrMaxPayload),
		"stream mismatch":      apiErr(10060),
		"message too large":    apiErr(10054),
		"stream not found":     apiErr(10059),
		"insufficient storage": apiErr(10047),
		"connection closed":    nats.ErrConnectionClosed,
		"unknown error":        errors.New("timeout"),
	}
	rejected := make(map[string]bool, len(errs))
	for name, err := range errs {
		rejected[name] = IsRejected(err)
	}
	require.Equal(t, map[string]bool{
		"max payload":          true,
		"stream mismatch":      true,
		"message too large":    true,
		"stream not found":     false,
		"insufficient storage": false,
		"connection closed":    false,
		"unknown error":        false,
	}, rejected)
}

func TestIsUnavailable(t *testing.T) {
	apiErr := func(code int, errorCode jetstream.ErrorCode) error {
		return fmt.Errorf("creating or updating stream: %w", &jetstream.APIError{Code: code, ErrorCode: errorCode})
	}
	errs := map[string]error{
		"cluster unavailable":  apiErr(503, 10008),
		"no responders":        fmt.Errorf("creating or updating stream: %w", jetstream.ErrJetStreamNotEnabled),
		"request timeout":      nats.ErrTimeout,
		"deadline":             context.DeadlineExceeded,
		"bad request":          apiErr(400, 10003),
		"stream name in use":   apiErr(400, 10058),
		"insufficient storage": apiErr(500, 10047),
		"unknown error":        errors.New("boom"),
	}
	unavailable := make(map[string]bool, len(errs))
	for name, err := range errs {
		unavailable[name] = IsUnavailable(err)
	}
	require.Equal(t, map[string]bool{
		"cluster unavailable":  true,
		"no responders":        true,
		"request timeout":      true,
		"deadline":             true,
		"bad request":          false,
		"stream name in use":   false,
		"insufficient storage": false,
		"unknown error":        false,
	}, unavailable)
}
