package k8s

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/manifest"
)

const testdata = "tools/onyx/k8s/testdata/"

func load(t *testing.T, filename string) *onyxpb.MainManifest {
	t.Helper()
	m := &onyxpb.MainManifest{}
	require.NoError(t, manifest.Load(testdata+filename, m))
	return m
}

func TestGenerate(t *testing.T) {
	files, err := Generate(load(t, "main.yaml"), "test")
	require.NoError(t, err)

	golden := testdata + "golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		for filename, data := range files {
			require.NoError(t, os.WriteFile(filepath.Join(golden, filename), data, 0o644))
		}
	}
	entries, err := os.ReadDir(golden)
	require.NoError(t, err)
	expected := map[string]string{}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(golden, entry.Name()))
		require.NoError(t, err)
		expected[entry.Name()] = string(data)
	}
	actual := map[string]string{}
	for filename, data := range files {
		actual[filename] = string(data)
	}
	require.Equal(t, expected, actual)
}

func TestGenerateRejectsPortCollision(t *testing.T) {
	// The gateway and the http server both default to 8080.
	_, err := Generate(load(t, "port_collision.yaml"), "test")
	require.ErrorContains(t, err, "a-service-grpc-gateway and d-http-http both bind port 8080")
}

func TestGenerateRejectsGatewayPortWithoutGateway(t *testing.T) {
	_, err := Generate(load(t, "gateway_port.yaml"), "test")
	require.ErrorContains(t, err, "server a-service: gateway_port is set but no service sets gateway")
}

func TestLoadRejectsProcessorPort(t *testing.T) {
	err := manifest.Load(testdata+"processor_port.yaml", &onyxpb.MainManifest{})
	require.ErrorContains(t, err, "a processor has no listener")
}

func TestLoadRejectsBadQuantity(t *testing.T) {
	err := manifest.Load(testdata+"bad_quantity.yaml", &onyxpb.MainManifest{})
	require.ErrorContains(t, err, "resources.requests.memory")
}
