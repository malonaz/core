package provider

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type stubProvider struct{ id string }

func (p *stubProvider) ProviderId() string          { return p.id }
func (p *stubProvider) Start(context.Context) error { return nil }
func (p *stubProvider) Stop()                       {}

// A config is only evaluated when its provider is registered, which takes its API key:
// this is what catches a broken one before a deploy does.
func TestRegisterProviderConfigs(t *testing.T) {
	entries, err := configsFS.ReadDir("configs")
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		providerID := strings.TrimSuffix(entry.Name(), ".json")
		t.Run(providerID, func(t *testing.T) {
			modelService, err := NewModelService()
			require.NoError(t, err)
			require.NoError(t, modelService.RegisterProvider(context.Background(), &stubProvider{id: providerID}))
		})
	}
}
