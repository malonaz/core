package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"go/certs"
	"go/health"
	"go/prometheus"
)

const (
	// maxRetries is the number of type we retry a retryable client
	maxRetries = 5
	// retryBackoff is the default timeout we apply between retries for a retryable client.
	retryBackoff = 100 * time.Millisecond
	// defaultTimeout defines the default client timeout for RPCs.
	defaultTimeout = 10 * time.Second
)

var (
	// DisableLogging is used by cli application to avoid messing with stdout.
	DisableLogging = false

	// DefaultRetriableCodes is a set of well known types gRPC codes that should be retri-able.
	// `ResourceExhausted` means that the user quota, e.g. per-RPC limits, have been reached.
	// `Unavailable` means that system is currently unavailable and the client should retry again.
	retriableCodes = []codes.Code{codes.ResourceExhausted, codes.Unavailable}

	clientKeepAliveParameters = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
)

// Client is a gRPC client.
type Client struct {
	opts       Opts
	connection *grpc.ClientConn

	// The first interceptor is called first.
	unaryInterceptors []grpc.UnaryClientInterceptor
	// The first interceptor is called first.
	streamInterceptors []grpc.StreamClientInterceptor
	// Streaming retry is handled differently because it panics on client-side streaming (not supported).
	// We thus allow a client to disable it.
	withStreamRetry bool
	options         []grpc.DialOption
}

// NewClient creates and returns a new gRPC client.
func NewClient(opts Opts, certsOpts certs.Opts, prometheusOpts prometheus.Opts) *Client {
	client := &Client{
		opts:            opts,
		withStreamRetry: true,
	}

	// Default options.
	client.options = append(client.options, grpc.WithMaxMsgSize(maximumMessageSize), withDNSBalancer())
	if opts.DisableTLS {
		if !DisableLogging {
			log.Warningf("Starting gRPC client using insecure gRPC dial")
		}
		client.options = append(client.options, grpc.WithInsecure())
	} else {
		tlsConfig, err := certsOpts.ClientTLSConfig()
		if err != nil {
			log.Panicf("Could not load client TLS config: %v", err)
		}
		client.options = append(client.options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Default interceptors.
	if !prometheusOpts.Disable {
		client.unaryInterceptors = append(client.unaryInterceptors, grpc_prometheus.UnaryClientInterceptor)
		client.streamInterceptors = append(client.streamInterceptors, grpc_prometheus.StreamClientInterceptor)
		grpc_prometheus.EnableClientHandlingTimeHistogram()
	}
	client.unaryInterceptors = append(client.unaryInterceptors, unaryClientValidateInterceptor(), withTimeout, withUnaryRetry())
	return client
}

// WithoutStreamRetryInterceptor disables the stream retry interceptor for this client.
func (c *Client) WithoutStreamRetryInterceptor() *Client {
	c.withStreamRetry = false
	return c
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
func (c *Client) Connect() (*grpc.ClientConn, health.Check) {
	if c.withStreamRetry {
		// We put the retry interceptor first.
		c.streamInterceptors = append([]grpc.StreamClientInterceptor{withStreamRetry()}, c.streamInterceptors...)
	}

	// Chain interceptors.
	if len(c.unaryInterceptors) > 0 {
		c.options = append(c.options, grpc.WithChainUnaryInterceptor(c.unaryInterceptors...))
	}
	if len(c.streamInterceptors) > 0 {
		c.options = append(c.options, grpc.WithChainStreamInterceptor(c.streamInterceptors...))
	}

	// Connect.
	url := fmt.Sprintf("%s:%d", c.opts.Host, c.opts.Port)
	connection, err := grpc.Dial(url, c.options...)
	if err != nil {
		log.Panicf("Failed to dial grpc [%s]: %v", url, err)
	}
	if !DisableLogging {
		log.Infof("connected to gRPC server on [%s]", url)
	}
	c.connection = connection
	return c.connection, c.HealthCheck
}

// HealthCheck calls the `Check` method of the grpc server.
func (c *Client) HealthCheck(ctx context.Context) error {
	healthClient := grpc_health_v1.NewHealthClient(c.connection)
	request := &grpc_health_v1.HealthCheckRequest{}
	response, err := healthClient.Check(ctx, request)
	if err != nil {
		return err
	}
	if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("grpc health failed health check with status: %s", grpc_health_v1.HealthCheckResponse_ServingStatus_name[int32(response.GetStatus())])
	}
	return nil
}

// withUnaryRetry returns a gRPC DialOption that adds a default retrying interceptor to all unary RPC calls.
// Only retries on ResourceExhausted and Unavailable errors.
func withUnaryRetry() grpc.UnaryClientInterceptor {
	return grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithCodes(retriableCodes...),
	)
}

func withStreamRetry() grpc.StreamClientInterceptor {
	return grpc_retry.StreamClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithCodes(retriableCodes...),
	)
}

// withDNSBalancer returns gRPC DialOption that does client-side load balancing based on DNS.
func withDNSBalancer() grpc.DialOption {
	// Must set the grpc server address resolver to dns.
	kuberesolver.RegisterInCluster()
	serviceConfig := fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, roundrobin.Name)
	return grpc.WithDefaultServiceConfig(serviceConfig)
}

func withTimeout(
	ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	// We set a default timeout only if there isn't one already.
	if _, ok := ctx.Deadline(); !ok {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// unaryClientValidateInterceptor returns a new unary client interceptor that validates outgoing messages.
// Invalid messages will be rejected with `InvalidArgument` before sending the request to server.
func unaryClientValidateInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if v, ok := req.(validator); ok {
			if err := v.ValidateAll(); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
