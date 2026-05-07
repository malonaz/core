package grpc

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/malonaz/core/go/health"
)

// Opts holds a gRPC server opts.
type Opts struct {
	Health              *health.GRPCOpts `group:"Health" namespace:"health" env-namespace:"HEALTH"`
	Port                int              `long:"port" env:"PORT" description:"Port to serve gRPC on." default:"9090"`
	Host                string           `long:"host" env:"HOST" description:"Host for a client to connect to."`
	DisableTLS          bool             `long:"disable-tls" env:"DISABLE_TLS" description:"Set to true in order to disable TLS for this service."`
	GracefulStopTimeout int              `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
	SocketPath          string           `long:"socket-path" env:"SOCKET_PATH" description:"Unix socket path to use instead of TCP port"`
	EnableReflection    bool             `long:"enable-reflection" env:"ENABLE_REFLECTION" description:"enable the reflection api"`
}

// GatewayOpts holds a gRPC gateway server opts.
type GatewayOpts struct {
	Port                   int      `long:"port" env:"PORT" description:"Port to serve the GRPC gateway on." default:"8080"`
	Host                   string   `long:"host" env:"HOST" description:"Host for a client to connect to."`
	GracefulStopTimeout    int      `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
	AllowedIncomingHeaders []string `long:"allowed-incoming-headers" env:"ALLOWED_INCOMING_HEADERS" description:"Additional HTTP headers to forward to gRPC metadata." env-delim:","`
	AllowedOutgoingHeaders []string `long:"allowed-outgoing-headers" env:"ALLOWED_OUTGOING_HEADERS" description:"gRPC metadata keys to forward as HTTP response headers." env-delim:","`
}

// Returns true if this opts uses sockets.
func (o *Opts) useSocket() bool {
	return o.SocketPath != ""
}

// Returns the target endpoint.
func (o *Opts) Endpoint() string {
	if o.useSocket() {
		return "unix:" + o.SocketPath
	}
	return fmt.Sprintf("%s:%d", o.Host, o.Port)
}

func ParseOpts(endpoint string) (*Opts, error) {
	opts := &Opts{}

	switch {
	case strings.HasPrefix(endpoint, "unix:"):
		opts.SocketPath = strings.TrimPrefix(endpoint, "unix:")
		opts.DisableTLS = true
		return opts, nil

	case strings.HasPrefix(endpoint, "https://"):
		endpoint = strings.TrimPrefix(endpoint, "https://")

	case strings.HasPrefix(endpoint, "http://"):
		endpoint = strings.TrimPrefix(endpoint, "http://")
		opts.DisableTLS = true
	}

	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		opts.Host = endpoint
		if opts.DisableTLS {
			opts.Port = 80
		} else {
			opts.Port = 443
		}
		return opts, nil
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("parsing port %q: %w", portStr, err)
	}
	opts.Host = host
	opts.Port = port
	return opts, nil
}
