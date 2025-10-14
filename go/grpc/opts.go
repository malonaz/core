package grpc

import (
	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

// Opts holds a gRPC server opts.
type Opts struct {
	Port       int    `long:"port" env:"PORT" description:"Port to serve gRPC on." default:"9090"`
	Host       string `long:"host" env:"HOST" description:"Host for a client to connect to."`
	DisableTLS bool   `long:"disable-tls" env:"DISABLE_TLS" description:"Set to true in order to disable TLS for this service."`
}

// GatewayOpts holds a gRPC gateway server opts.
type GatewayOpts struct {
	GRPC Opts
	Host string `long:"gateway-host" description:"Host for a client to connect to"`
	Port int    `long:"gateway-port" description:"Port to serve gateway on." default:"8080"`
}
