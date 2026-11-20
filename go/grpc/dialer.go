package grpc

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/prometheus"
)

// Dialer dials gRPC endpoints as the binary hosting it does: one connection per endpoint, and a connection to one of
// the binary's own servers, an endpoint one of them listens on, with the options the binary gives those.
type Dialer struct {
	certsOpts         *certs.Opts
	prometheusOpts    *prometheus.Opts
	endpointToServer  map[string]*ServerOpts
	serverDialOptions []grpc.DialOption

	mutex                sync.Mutex
	endpointToConnection map[string]*Connection
}

// NewDialer returns a dialer of the binary serving the servers.
func NewDialer(certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, servers ...*ServerOpts) *Dialer {
	endpointToServer := make(map[string]*ServerOpts, len(servers))
	for _, server := range servers {
		endpointToServer[server.ClientOpts().Endpoint()] = server
	}
	return &Dialer{
		certsOpts:            certsOpts,
		prometheusOpts:       prometheusOpts,
		endpointToServer:     endpointToServer,
		endpointToConnection: map[string]*Connection{},
	}
}

// WithServerOptions adds options to the connections to the binary's own servers, and to no other.
func (d *Dialer) WithServerOptions(options ...grpc.DialOption) *Dialer {
	d.serverDialOptions = append(d.serverDialOptions, options...)
	return d
}

// Dial returns the connection to the endpoint, opened on first use. One of the binary's own servers is dialed as the
// server itself says, whatever the opts say of it.
func (d *Dialer) Dial(ctx context.Context, opts *ClientOpts) (*Connection, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	endpoint := opts.Endpoint()
	if connection, ok := d.endpointToConnection[endpoint]; ok {
		return connection, nil
	}
	var options []grpc.DialOption
	if server, ok := d.endpointToServer[endpoint]; ok {
		opts, options = server.ClientOpts(), d.serverDialOptions
	}
	connection, err := NewConnection(opts, d.certsOpts, d.prometheusOpts)
	if err != nil {
		return nil, err
	}
	if err := connection.WithOptions(options...).Connect(ctx); err != nil {
		return nil, err
	}
	d.endpointToConnection[endpoint] = connection
	return connection, nil
}

// Close closes every connection.
func (d *Dialer) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	for endpoint, connection := range d.endpointToConnection {
		delete(d.endpointToConnection, endpoint)
		connection.Close()
	}
}
