package grpc

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/textproto"
	"reflect"
	"strings"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
	"github.com/malonaz/core/go/certs"
	grpc_interceptor "github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/prometheus"
)

const (
	HeaderXForwardedProto = "X-Forwarded-Proto"
	HeaderXForwardedHost  = "X-Forwarded-Host"
	HeaderXOriginalURL    = "X-Original-Url"
	HeaderXRequestBody    = "X-Request-Body-Bin"
)

// RegisterHandler is syntactice sugar for a gRPC gateway handler.
type RegisterHandler = func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error)

// Gateway is a gRPC gateway.
type Gateway struct {
	log *slog.Logger
	// Opts for this gateway.
	opts *GatewayOpts
	// Opts for the grpc server this gateway is connecting to.
	grpcOpts *Opts
	// Opts for certs.
	certsOpts *certs.Opts
	// Opts for prometheus.
	prometheusOpts *prometheus.Opts

	// HTTP Server.
	httpServer *http.Server

	// Handlers.
	registerHandlers []RegisterHandler

	// HTTP.
	options []runtime.ServeMuxOption

	// GRPC Client.
	dialOptions []grpc.DialOption

	// First interceptor is executed first.
	unaryInterceptors  []grpc.UnaryClientInterceptor
	streamInterceptors []grpc.StreamClientInterceptor

	// Gateway options.
	rpcMethodToGatewayOptions map[string]*grpcpb.GatewayOptions

	// Allowed headers.
	allowedOutgoingHeaderSet map[string]struct{}
	allowedIncomingHeaderSet map[string]struct{}
}

func (g *Gateway) WithLogger(logger *slog.Logger) *Gateway {
	g.log = logger
	return g
}

// NewGateway creates and returns a new Gateway.
func NewGateway(opts *GatewayOpts, grpcOpts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, registerHandlers []RegisterHandler) *Gateway {
	allowedOutgoingHeaderSet := make(map[string]struct{}, len(opts.AllowedOutgoingHeaders))
	for _, h := range opts.AllowedOutgoingHeaders {
		allowedOutgoingHeaderSet[textproto.CanonicalMIMEHeaderKey(h)] = struct{}{}
	}
	allowedIncomingHeaderSet := make(map[string]struct{}, len(opts.AllowedIncomingHeaders))
	for _, h := range opts.AllowedIncomingHeaders {
		allowedIncomingHeaderSet[textproto.CanonicalMIMEHeaderKey(h)] = struct{}{}
	}
	return &Gateway{
		log:                      slog.Default(),
		opts:                     opts,
		grpcOpts:                 grpcOpts,
		certsOpts:                certsOpts,
		prometheusOpts:           prometheusOpts,
		registerHandlers:         registerHandlers,
		allowedOutgoingHeaderSet: allowedOutgoingHeaderSet,
		allowedIncomingHeaderSet: allowedIncomingHeaderSet,
	}
}

// WithOptions adds options to this gRPC gateway.
func (g *Gateway) WithOptions(options ...runtime.ServeMuxOption) *Gateway {
	g.options = append(g.options, options...)
	return g
}

// WithDialOptions adds dial options to this gRPC gateway.
func (g *Gateway) WithDialOptions(options ...grpc.DialOption) *Gateway {
	g.dialOptions = append(g.dialOptions, options...)
	return g
}

// WithClientUnaryInterceptors adds interceptors to this gateway's gRPC client..
func (g *Gateway) WithClientUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) *Gateway {
	g.unaryInterceptors = append(g.unaryInterceptors, interceptors...)
	return g
}

// WithClientStreamInterceptors adds interceptors to this gateway's gRPC client..
func (g *Gateway) WithClientStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) *Gateway {
	g.streamInterceptors = append(g.streamInterceptors, interceptors...)
	return g
}

// Serve serves this gRPC gateway. Blocking call.
func (g *Gateway) Serve(ctx context.Context) error {
	g.log = g.log.WithGroup("grpc_gateway_server").With(
		"port", g.opts.Port, "host", g.opts.Host,
		slog.Group("grpc_server",
			"port", g.grpcOpts.Port, "host", g.grpcOpts.Host, "socket_path", g.grpcOpts.SocketPath,
			"disable_tls", g.grpcOpts.DisableTLS,
		),
	)
	gatewayCookie := &GatewayCookie{log: g.log}
	// Some default options.
	g.options = append(
		g.options,
		runtime.WithOutgoingHeaderMatcher(g.outgoingHeaderMatcher),
		runtime.WithIncomingHeaderMatcher(g.incomingHeaderMatcher),
		runtime.WithForwardResponseOption(gatewayCookie.forwardOutOption),
		runtime.WithForwardResponseOption(forwardResponseOptionHTTPHeadersForwarder),
		runtime.WithMetadata(gatewayCookie.forwardInOption),
		runtime.WithMetadata(g.gatewayOptionsMetadata),
	)
	g.options = append(g.options, withCustomMarshalers()...)

	// Default dial options.
	messageSizeDialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MaximumMessageSize),
		grpc.MaxCallSendMsgSize(MaximumMessageSize),
	)
	g.dialOptions = append(g.dialOptions, messageSizeDialOptions)
	// If we use sockets, we do not use the load balancer. Note that we basically always hit localhost so not very useful.
	if !g.grpcOpts.useSocket() {
		g.dialOptions = append(g.dialOptions, WithDNSBalancer())
	}

	// Handle TLS / Plaintext configuration.
	clientTransportCredentialsOptions, err := getClientTransportCredentialsOptions(g.grpcOpts, g.certsOpts)
	if err != nil {
		return err
	}
	g.dialOptions = append(g.dialOptions, clientTransportCredentialsOptions)

	// Default interceptors.
	g.unaryInterceptors = append(g.unaryInterceptors, grpc_interceptor.UnaryClientRetry())
	if g.prometheusOpts.Enabled() {
		metrics := grpc_prometheus.NewClientMetrics(
			grpc_prometheus.WithClientHandlingTimeHistogram(
				grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
			),
		)
		g.unaryInterceptors = append(g.unaryInterceptors, metrics.UnaryClientInterceptor())
		g.streamInterceptors = append(g.streamInterceptors, metrics.StreamClientInterceptor())
	}

	// Chain interceptors.
	if len(g.unaryInterceptors) > 0 {
		g.dialOptions = append(g.dialOptions, grpc.WithChainUnaryInterceptor(g.unaryInterceptors...))
	}
	if len(g.streamInterceptors) > 0 {
		g.dialOptions = append(g.dialOptions, grpc.WithChainStreamInterceptor(g.streamInterceptors...))
	}
	mux := runtime.NewServeMux(g.options...)
	endpoint := g.grpcOpts.Endpoint()
	for _, registerHandler := range g.registerHandlers {
		if err := registerHandler(ctx, mux, endpoint, g.dialOptions); err != nil {
			return fmt.Errorf("registering handler: %w", err)
		}
	}

	// Get the gateway options.
	g.rpcMethodToGatewayOptions, err = getMethodNameToGatewayOptions()
	if err != nil {
		return fmt.Errorf("getting gateway options: %w", err)
	}

	url := fmt.Sprintf(":%d", g.opts.Port)
	handler := allowCORS(mux)
	g.httpServer = &http.Server{Addr: url, Handler: handler}
	g.log.InfoContext(ctx, "serving")
	if err := g.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("exited unexpectedly: %w", err)
	}
	return nil
}

// Stop immediately stops the gateway server.
func (g *Gateway) Stop() error {
	g.log.Info("stopping")
	if g.httpServer != nil {
		return g.httpServer.Close()
	}
	return nil
}

// GracefulStop gracefully stops the gateway server.
func (g *Gateway) GracefulStop() error {
	g.log.Info("gracefully stopping")
	if g.httpServer != nil {
		duration := time.Duration(g.opts.GracefulStopTimeout) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		err := g.httpServer.Shutdown(ctx)
		if err == context.DeadlineExceeded {
			g.log.Warn("graceful shutdown timed out")
			// Force close any remaining connections
			return g.Stop()
		}
		return err
	}
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////// VARIOUS GATEWAY OPTIONS BELOW //////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
func (g *Gateway) outgoingHeaderMatcher(key string) (string, bool) {
	if _, ok := g.allowedOutgoingHeaderSet[textproto.CanonicalMIMEHeaderKey(key)]; ok {
		return key, true
	}
	// Default: prefix with Grpc-Metadata- to prevent clashing with http internal headers.
	return runtime.MetadataHeaderPrefix + key, true
}

func (g *Gateway) incomingHeaderMatcher(key string) (string, bool) {
	// Fallback to the default matcher.
	replacement, ok := runtime.DefaultHeaderMatcher(key)
	if ok {
		return replacement, true
	}

	canonicalKey := textproto.CanonicalMIMEHeaderKey(key)
	if _, ok := g.allowedIncomingHeaderSet[canonicalKey]; ok {
		return canonicalKey, true
	}

	return "", false
}

// allowCORS allows Cross Origin Resource Sharing from any origin.
func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			if r.Method == "OPTIONS" && r.Header.Get("Access-Control-Request-Method") != "" {
				preflightHandler(w, r)
				return
			}
		}
		if allowCredentials := r.Header.Get("Access-Control-Allow-Credentials"); allowCredentials == "true" {
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		h.ServeHTTP(w, r)
	})
}

func (g *Gateway) gatewayOptionsMetadata(ctx context.Context, r *http.Request) metadata.MD {
	// Get the rpc method.
	rpcMethod, ok := runtime.RPCMethod(ctx)
	if !ok {
		return nil
	}

	// Get the gateway options.
	gatewayOptions, ok := g.rpcMethodToGatewayOptions[rpcMethod]
	if !ok {
		return nil
	}

	// Process the options.
	md := metadata.MD{}
	if gatewayOptions.GetRequireOriginalUrl() {
		scheme := r.Header.Get(HeaderXForwardedProto)
		if scheme == "" {
			scheme = "http"
			if r.TLS != nil {
				scheme = "https"
			}
		}
		host := r.Header.Get(HeaderXForwardedHost)
		if host == "" {
			host = r.Host
		}
		md.Set(HeaderXOriginalURL, scheme+"://"+host+r.URL.String())
	}
	if gatewayOptions.GetRequireRequestBody() && r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		md.Set(HeaderXRequestBody, string(body))
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	return md
}

func customMimeWrapper(routeToGatewayOptions map[string]*grpcpb.GatewayOptions, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayOptions := routeToGatewayOptions[r.URL.Path]
		if gatewayOptions.GetCustomMime() != "" {
			r.Header.Set("Content-Type", gatewayOptions.CustomMime)
		}
		h.ServeHTTP(w, r)
	})
}

func preflightHandler(w http.ResponseWriter, r *http.Request) {
	// Allow whatever headers the client is requesting
	if requestHeaders := r.Header.Get("Access-Control-Request-Headers"); requestHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", requestHeaders)
	}

	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

var grpcGatewayMarshalerOptions = &runtime.JSONPb{
	MarshalOptions:   pbutil.JsonMarshalOptions,
	UnmarshalOptions: pbutil.JsonUnmarshalOptions,
}

var grpcGatewayMarshalerCamelCaseOptions = &runtime.JSONPb{
	MarshalOptions:   pbutil.JsonCamelCaseMarshalOptions,
	UnmarshalOptions: pbutil.JsonUnmarshalOptions,
}

func withCustomMarshalers() []runtime.ServeMuxOption {
	return []runtime.ServeMuxOption{
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{
			Marshaler: grpcGatewayMarshalerOptions,
		}),
		runtime.WithMarshalerOption("application/json+camel", &runtime.HTTPBodyMarshaler{
			Marshaler: grpcGatewayMarshalerCamelCaseOptions,
		}),
		runtime.WithMarshalerOption("application/x-www-form-urlencoded", &runtime.HTTPBodyMarshaler{
			Marshaler: &urlEncodedMarshaler{},
		}),
		runtime.WithMarshalerOption("application/raw-webhook", &runtime.HTTPBodyMarshaler{
			Marshaler: &rawJSONPb{grpcGatewayMarshalerOptions},
		}),
	}
}

// /////////////  FORWARD RESPONSE OPTIONS //////////////
const (
	HTTPHeaderLocation     = "Location"
	HTTPHeaderCacheControl = "Cache-Control"
)

var (
	httpHeaderSet = map[string]struct{}{
		HTTPHeaderLocation:     {},
		HTTPHeaderCacheControl: {},
	}
)

func SetHTTPHeader(ctx context.Context, key, value string) error {
	md := metadata.MD{}
	md.Append(key, value)
	return grpc.SetHeader(ctx, md)
}

func forwardResponseOptionHTTPHeadersForwarder(ctx context.Context, w http.ResponseWriter, resp proto.Message) error {
	headers := w.Header()
	for key := range httpHeaderSet {
		if values, ok := headers[runtime.MetadataHeaderPrefix+key]; ok {
			w.Header().Set(key, values[0])
			if key == HTTPHeaderLocation {
				w.WriteHeader(http.StatusFound)
			}
		}
	}

	return nil
}

// /////////////  WEBHOOK UNMARSHALER //////////////
var typeOfBytes = reflect.TypeOf([]byte(nil))

type rawJSONPb struct {
	*runtime.JSONPb
}

func (*rawJSONPb) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(func(v any) error {
		rawData, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		rv := reflect.ValueOf(v)

		if rv.Kind() != reflect.Ptr {
			return fmt.Errorf("%T is not a pointer", v)
		}

		rv = rv.Elem()
		if rv.Type() != typeOfBytes {
			return fmt.Errorf("Type must be []byte but got %T", v)
		}

		rv.Set(reflect.ValueOf(rawData))
		return nil
	})
}
