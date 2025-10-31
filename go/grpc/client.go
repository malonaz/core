package grpc

import (
	"context"
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
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

// Client is a gRPC client.
type Client struct {
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

// NewClient creates and returns a new gRPC client.
func NewClient(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts) *Client {
	client := &Client{
		opts: opts,
	}

	validator, err := protovalidate.New()
	if err != nil {
		log.Panicf("could not instantiate proto validator")
	}

	// Default options.
	client.options = append(client.options, grpc.WithMaxMsgSize(MaximumMessageSize))
	if !opts.useSocket() {
		client.options = append(client.options, WithDNSBalancer())
	}
	if opts.DisableTLS {
		log.Warningf("Starting gRPC client using insecure gRPC dial")
		client.options = append(client.options, grpc.WithInsecure())
	} else {
		tlsConfig, err := certsOpts.ClientTLSConfig()
		if err != nil {
			log.Panicf("Could not load client TLS config: %v", err)
		}
		client.options = append(client.options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Default interceptors.
	client.preUnaryInterceptors = append(client.preUnaryInterceptors, interceptor.UnaryClientTrailerPropagation())
	client.preStreamInterceptors = append(client.preStreamInterceptors, interceptor.StreamClientTrailerPropagation())

	if !prometheusOpts.Disable {
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
	return client
}

// WithOptions adds options to this gRPC client.
func (c *Client) WithOptions(options ...grpc.DialOption) *Client {
	c.options = append(c.options, options...)
	return c
}

// WithUnaryInterceptors adds interceptors to this gRPC client.
func (c *Client) WithUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) *Client {
	c.unaryInterceptors = append(c.unaryInterceptors, interceptors...)
	return c
}

// WithStreamInterceptors adds interceptors to this gRPC client.
func (c *Client) WithStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) *Client {
	c.streamInterceptors = append(c.streamInterceptors, interceptors...)
	return c
}

// Connect dials the gRPC connection and returns it, as well as a health.ProbeFN, to encourage
// any client to use the probe fn as a health check.
func (c *Client) Connect() *grpc.ClientConn {
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
	connection, err := grpc.Dial(endpoint, c.options...)
	if err != nil {
		log.Panicf("Failed to dial grpc [%s]: %v", endpoint, err)
	}
	log.Infof("connected to gRPC server on [%s]", endpoint)
	c.connection = connection
	return c.connection
}

// HealthCheck calls the `Check` method of the grpc server.
func (c *Client) HealthCheckFn(service string) health.Check {
	return func(ctx context.Context) error {
		healthClient := grpc_health_v1.NewHealthClient(c.connection)
		request := &grpc_health_v1.HealthCheckRequest{Service: service}
		response, err := healthClient.Check(ctx, request)
		if err != nil {
			return err
		}
		if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("grpc health failed health check with status: %s", grpc_health_v1.HealthCheckResponse_ServingStatus_name[int32(response.GetStatus())])
		}
		return nil
	}
}

// withDNSBalancer returns gRPC DialOption that does client-side load balancing based on DNS.
func WithDNSBalancer() grpc.DialOption {
	// Must set the grpc server address resolver to dns.
	kuberesolver.RegisterInCluster()
	serviceConfig := fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, roundrobin.Name)
	return grpc.WithDefaultServiceConfig(serviceConfig)
}
