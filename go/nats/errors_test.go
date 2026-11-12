package nats

import (
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
		"max payload":           fmt.Errorf("publishing message: %w", nats.ErrMaxPayload),
		"stream mismatch":       apiErr(10060),
		"message too large":     apiErr(10054),
		"stream not found":      apiErr(10059),
		"insufficient storage":  apiErr(10047),
		"connection closed":     nats.ErrConnectionClosed,
		"unknown error":         errors.New("timeout"),
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
