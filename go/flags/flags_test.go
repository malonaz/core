package flags

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type serverOpts struct {
	Port       int    `long:"port" env:"PORT" default:"9090"`
	SocketPath string `long:"socket-path" env:"SOCKET_PATH"`
	DisableTLS bool   `long:"disable-tls" env:"DISABLE_TLS"`
}

type testOpts struct {
	A *serverOpts `group:"A" namespace:"a-grpc" env-namespace:"A_GRPC"`
	B *serverOpts `group:"B" namespace:"b-grpc" env-namespace:"B_GRPC"`
}

func TestParseWithDefaults(t *testing.T) {
	os.Args = []string{"test"}
	t.Setenv("B_GRPC_PORT", "7070")
	opts := &testOpts{}
	require.NoError(t, ParseWithDefaults(opts, map[string]string{
		"a-grpc.socket-path": "/tmp/a.socket",
		"a-grpc.disable-tls": "true",
		"b-grpc.port":        "45322",
	}))
	// Env wins over the replaced default.
	require.Equal(t, &testOpts{
		A: &serverOpts{Port: 9090, SocketPath: "/tmp/a.socket", DisableTLS: true},
		B: &serverOpts{Port: 7070},
	}, opts)
}

func TestParseWithDefaultsRejectsUnknownFlag(t *testing.T) {
	os.Args = []string{"test"}
	err := ParseWithDefaults(&testOpts{}, map[string]string{"c-grpc.port": "1"})
	require.ErrorContains(t, err, "no flag --c-grpc.port to default")
}
