package grpc

import (
	"fmt"

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
	Port                int    `long:"port" env:"PORT" description:"Port to serve the GRPC gateway on." default:"8080"`
	Host                string `long:"host" env:"HOST" description:"Host for a client to connect to."`
	GracefulStopTimeout int    `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
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
