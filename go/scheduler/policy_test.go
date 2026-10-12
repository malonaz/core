package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryBackoffOpts_Proto(t *testing.T) {
	t.Run("built-in default", func(t *testing.T) {
		retryBackoff, err := (&RetryBackoffOpts{Initial: 10 * time.Second, Max: 10 * time.Minute, Multiplier: 2}).Proto()
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, retryBackoff.GetInitial().AsDuration())
		require.Equal(t, 10*time.Minute, retryBackoff.GetMax().AsDuration())
		require.Equal(t, float64(2), retryBackoff.GetMultiplier())
	})

	// A default no queue policy would accept fails at startup, not on the first retry.
	for name, opts := range map[string]*RetryBackoffOpts{
		"zero initial":         {Max: time.Minute, Multiplier: 2},
		"zero max":             {Initial: time.Second, Multiplier: 2},
		"shrinking multiplier": {Initial: time.Second, Max: time.Minute, Multiplier: 0.5},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := opts.Proto()
			require.Error(t, err)
		})
	}
}
