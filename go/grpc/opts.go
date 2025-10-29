package grpc

import (
	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

// Opts holds a gRPC server opts.
type Opts struct {
	Port                int    `long:"port" env:"PORT" description:"Port to serve gRPC on." default:"9090"`
	Host                string `long:"host" env:"HOST" description:"Host for a client to connect to."`
	DisableTLS          bool   `long:"disable-tls" env:"DISABLE_TLS" description:"Set to true in order to disable TLS for this service."`
	GracefulStopTimeout int    `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
}

// GatewayOpts holds a gRPC gateway server opts.
type GatewayOpts struct {
	Port int    `long:"port" env:"PORT" description:"Port to serve gRPC on." default:"8080"`
	Host string `long:"host" env:"HOST" description:"Host for a client to connect to."`
}
