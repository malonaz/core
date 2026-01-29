package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/prometheus"
)

var (
	clientKeepAliveParameters = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
)

// Connection is a gRPC client.
type Connection struct {
	log        *slog.Logger
	opts       *Opts
	connection *grpc.ClientConn

	// The **first** interceptor is the **outermost** (executes first on request, last on response).
	// Order of interceptors is [PRE_OPTIONS_DEFAULT, OPTIONS, POST_OPTIONS_DEFAULT].
	preUnaryInterceptors   []grpc.UnaryClientInterceptor
	unaryInterceptors      []grpc.UnaryClientInterceptor
	postUnaryInterceptors  []grpc.UnaryClientInterceptor
	preStreamInterceptors  []grpc.StreamClientInterceptor
	streamInterceptors     []grpc.StreamClientInterceptor
	postStreamInterceptors []grpc.StreamClientInterceptor

	options []grpc.DialOption
}

func (c *Connection) WithLogger(logger *slog.Logger) *Connection {
	c.log = logger
	return c
}

func getClientTransportCredentialsOptions(opts *Opts, certsOpts *certs.Opts) (grpc.DialOption, error) {
	if opts.DisableTLS {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	var tlsConfig *tls.Config
	if certsOpts == nil {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	} else {
		var err error
		tlsConfig, err = certsOpts.ClientTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading client TLS config: %w", err)
		}
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

// NewConnection creates and returns a new gRPC client.
func NewConnection(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts) (*Connection, error) {
	client := &Connection{
		log:  slog.Default(),
		opts: opts,
	}

	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("instantiating proto validator: %w", err)
	}

	// Default options.
	client.options = append(client.options, grpc.WithMaxMsgSize(MaximumMessageSize))
	if !opts.useSocket() {
		client.options = append(client.options, WithDNSBalancer())
	}

	// Handle TLS / Plaintext configuration.
	clientTransportCredentialsOptions, err := getClientTransportCredentialsOptions(opts, certsOpts)
	if err != nil {
		return nil, err
	}
	client.options = append(client.options, clientTransportCredentialsOptions)

	// Default interceptors.
	client.preUnaryInterceptors = append(client.preUnaryInterceptors, interceptor.UnaryClientTrailerPropagation())
	client.preStreamInterceptors = append(client.preStreamInterceptors, interceptor.StreamClientTrailerPropagation())

	if prometheusOpts.Enabled() {
		metrics := grpc_prometheus.NewClientMetrics(
			grpc_prometheus.WithClientHandlingTimeHistogram(
				grpc_prometheus.WithHistogramBuckets(prometheusDefaultHistogramBuckets),
			),
		)
		client.preUnaryInterceptors = append(client.preUnaryInterceptors, metrics.UnaryClientInterceptor())
		client.preStreamInterceptors = append(client.preStreamInterceptors, metrics.StreamClientInterceptor())
	}

	// Post interceptors.
	client.postUnaryInterceptors = append(
		client.postUnaryInterceptors,
		interceptor.UnaryClientValidate(validator),
		interceptor.UnaryClientTimeout(),
		interceptor.UnaryClientRetry(),
	)
	client.postStreamInterceptors = append(client.postStreamInterceptors, interceptor.StreamClientRetry())
	return client, nil
}

// WithOptions adds options to this gRPC client.
func (c *Connection) WithOptions(options ...grpc.DialOption) *Connection {
	c.options = append(c.options, options...)
	return c
}

// WithUnaryInterceptors adds interceptors to this gRPC client.
func (c *Connection) WithUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) *Connection {
	c.unaryInterceptors = append(c.unaryInterceptors, interceptors...)
	return c
}

// WithStreamInterceptors adds interceptors to this gRPC client.
func (c *Connection) WithStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) *Connection {
	c.streamInterceptors = append(c.streamInterceptors, interceptors...)
	return c
}

// Connect dials the gRPC connection and returns it, as well as a health.ProbeFN, to encourage
// any client to use the probe fn as a health check.
func (c *Connection) Connect(ctx context.Context) error {
	c.log = c.log.WithGroup("grpc_client").With(
		"port", c.opts.Port, "host", c.opts.Host, "socket_path", c.opts.SocketPath,
		"disable_tls", c.opts.DisableTLS,
	)
	unaryInterceptors := append(c.preUnaryInterceptors, c.unaryInterceptors...)
	unaryInterceptors = append(unaryInterceptors, c.postUnaryInterceptors...)
	streamInterceptors := append(c.preStreamInterceptors, c.streamInterceptors...)
	streamInterceptors = append(streamInterceptors, c.postStreamInterceptors...)

	// Chain interceptors.
	if len(unaryInterceptors) > 0 {
		c.options = append(c.options, grpc.WithChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		c.options = append(c.options, grpc.WithChainStreamInterceptor(streamInterceptors...))
	}

	// Connect.
	endpoint := c.opts.Endpoint()
	connection, err := grpc.DialContext(ctx, endpoint, c.options...)
	if err != nil {
		return fmt.Errorf("dialing grpc [%s]: %v", endpoint, err)
	}
	c.log.InfoContext(ctx, "connected")
	c.connection = connection
	return nil
}

func (c *Connection) Close() error {
	if c.connection != nil {
		return c.connection.Close()
	}
	return nil
}

func (c *Connection) Get() *grpc.ClientConn {
	return c.connection
}

// HealthCheck calls the `Check` method of the grpc server, specifying the service.
func (c *Connection) HealthCheckFn(service string) health.Check {
	healthClient := grpc_health_v1.NewHealthClient(c.connection)
	return func(ctx context.Context) error {
		request := &grpc_health_v1.HealthCheckRequest{Service: service}
		response, err := healthClient.Check(ctx, request)
		if err != nil {
			return err
		}
		if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("health check returned :%s", response.Status)
		}
		return nil
	}
}

var kuberesolverMutex sync.Mutex // kuberesolver's Register function is not thread safe.
// withDNSBalancer returns gRPC DialOption that does client-side load balancing based on DNS.
func WithDNSBalancer() grpc.DialOption {
	// Must set the grpc server address resolver to dns.
	kuberesolverMutex.Lock()
	kuberesolver.RegisterInCluster()
	kuberesolverMutex.Unlock()
	serviceConfig := fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, roundrobin.Name)
	return grpc.WithDefaultServiceConfig(serviceConfig)
}
