package grpc

import (
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"reflect"
	"strings"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	grpcpb "github.com/malonaz/core/genproto/grpc"
	"github.com/malonaz/core/go/certs"
	grpc_interceptor "github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/prometheus"
)

const (
	grpcGatewayContextMetadataHTTPPathPatternKey = "http-path-pattern"
)

var grpcGatewayMarshalerOptions = &runtime.JSONPb{
	MarshalOptions:   pbutil.ProtoJsonMarshalOptions,
	UnmarshalOptions: pbutil.ProtoJsonUnmarshalOptions,
}

// RegisterHandler is syntactice sugar for a gRPC gateway handler.
type RegisterHandler = func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error)

// Gateway is a gRPC gateway.
type Gateway struct {
	// Opts for this gateway.
	opts *GatewayOpts
	// Opts for the grpc server this gateway is connecting to.
	grpcOpts *Opts

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

	// Raw routes.
	routeToGatewayOptions map[string]*grpcpb.GatewayOptions
}

// NewGateway creates and returns a new Gateway.
func NewGateway(opts *GatewayOpts, grpcOpts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, registerHandlers []RegisterHandler) (*Gateway, error) {
	gateway := &Gateway{
		opts:             opts,
		grpcOpts:         grpcOpts,
		registerHandlers: registerHandlers,
		options: []runtime.ServeMuxOption{
			// Some default options.
			runtime.WithOutgoingHeaderMatcher(outgoingHeaderMatcher),
			runtime.WithIncomingHeaderMatcher(incomingHeaderMatcher),
			runtime.WithForwardResponseOption(GatewayCookie{}.forwardOutOption),
			runtime.WithForwardResponseOption(forwardResponseOptionHTTPHeadersForwarder),
			runtime.WithMetadata(GatewayCookie{}.forwardInOption),
			runtime.WithMarshalerOption("application/raw-webhook", &rawJSONPb{grpcGatewayMarshalerOptions}),
			withCustomMarshaler(),
			withHTTPPatternAnnotation(),
		},
	}

	// Default dial options.
	messageSizeDialOptions := grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MaximumMessageSize),
		grpc.MaxCallSendMsgSize(MaximumMessageSize),
	)
	gateway.dialOptions = append(gateway.dialOptions, messageSizeDialOptions)
	// If we use sockets, we do not use the load balancer. Note that we basically always hit localhost so not very useful.
	if !grpcOpts.useSocket() {
		gateway.dialOptions = append(gateway.dialOptions, WithDNSBalancer())
	}
	if grpcOpts.DisableTLS {
		log.Warningf("Starting gRPC client using insecure gRPC dial")
		gateway.dialOptions = append(gateway.dialOptions, grpc.WithInsecure())
	} else {
		tlsConfig, err := certsOpts.ClientTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading client TLS config: %w", err)
		}
		gateway.dialOptions = append(gateway.dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Default interceptors.
	gateway.unaryInterceptors = append(gateway.unaryInterceptors, grpc_interceptor.UnaryClientRetry())
	if !prometheusOpts.Disable {
		metrics := grpc_prometheus.NewClientMetrics(
			grpc_prometheus.WithClientHandlingTimeHistogram(
				grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
			),
		)
		gateway.unaryInterceptors = append(gateway.unaryInterceptors, metrics.UnaryClientInterceptor())
		gateway.streamInterceptors = append(gateway.streamInterceptors, metrics.StreamClientInterceptor())
	}
	return gateway, nil
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
	log.Infof("Connected to gRPC server on [%s]", endpoint)

	var protoRegistryRangeFileErr error
	g.routeToGatewayOptions = map[string]*grpcpb.GatewayOptions{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				if !proto.HasExtension(method.Options(), grpcpb.E_GatewayOptions) {
					continue
				}
				if !proto.HasExtension(method.Options(), annotations.E_Http) {
					continue
				}
				httpRule := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
				gatewayOptions := proto.GetExtension(method.Options(), grpcpb.E_GatewayOptions).(*grpcpb.GatewayOptions)
				var path string
				switch pattern := httpRule.Pattern.(type) {
				case *annotations.HttpRule_Get:
					path = pattern.Get
				case *annotations.HttpRule_Post:
					path = pattern.Post
				case *annotations.HttpRule_Put:
					path = pattern.Put
				case *annotations.HttpRule_Delete:
					path = pattern.Delete
				case *annotations.HttpRule_Patch:
					path = pattern.Patch
				case *annotations.HttpRule_Custom:
					path = pattern.Custom.Path
				}
				if path == "" {
					protoRegistryRangeFileErr = fmt.Errorf("no path defined in %v", httpRule)
					return false
				}
				g.routeToGatewayOptions[path] = gatewayOptions
				log.Infof("[%s] registered gateway options: %+v", path, gatewayOptions)
			}
		}
		return true
	})
	if protoRegistryRangeFileErr != nil {
		return protoRegistryRangeFileErr
	}

	url := fmt.Sprintf(":%d", g.opts.Port)
	handler := customMimeWrapper(g.routeToGatewayOptions, allowCORS(mux))
	g.httpServer = &http.Server{Addr: url, Handler: handler}
	log.Infof("Serving gRPC Gateway on port [:%d]", g.opts.Port)
	if err := g.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("gateway server exited unexpectedly: %w", err)
	}
	return nil
}

// Stop immediately stops the gateway server.
func (g *Gateway) Stop() error {
	if g.httpServer == nil {
		return nil
	}
	log.Info("Stopping gRPC Gateway")
	return g.httpServer.Close()
}

// GracefulStop gracefully stops the gateway server.
func (g *Gateway) GracefulStop() error {
	if g.httpServer == nil {
		return nil
	}
	log.Info("Gracefully stopping gRPC Gateway")
	duration := time.Duration(g.opts.GracefulStopTimeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	err := g.httpServer.Shutdown(ctx)
	if err == context.DeadlineExceeded {
		log.Warning("Graceful shutdown timed out")
		// Force close any remaining connections
		return g.Stop()
	}
	return err
}

// /////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////// VARIOUS GATEWAY OPTIONS BELOW //////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
func outgoingHeaderMatcher(key string) (string, bool) {
	// This is the default in runtime.go.
	// This is important to prevent grpc headers from clashing with http internal headers.
	// We use response forwarders to clean this up.
	return runtime.MetadataHeaderPrefix + key, true
}

var allowedIncomingHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("X-Telegram-Bot-Api-Secret-Token"): {},
}

func incomingHeaderMatcher(key string) (string, bool) {
	// Fallback to the default matcher.
	replacement, ok := runtime.DefaultHeaderMatcher(key)
	if ok {
		return replacement, true
	}

	canonicalKey := textproto.CanonicalMIMEHeaderKey(key)
	// If default matcher rejects, we check if we allow it.
	if _, ok := allowedIncomingHeaders[canonicalKey]; ok {
		// Note that this is what the default behaviour of the gRPC library does (where all headers are allowed).
		return runtime.MetadataHeaderPrefix + canonicalKey, true
	}
	// Otherwise reject.
	return "", false
}

// GetHTTPPathPatternFromContext returns the http path pattern if it exists.
// This can only be used in grpc gateway handlers, where the pattern is injected into the context's grpc.MD.
func GetHTTPPathPatternFromContext(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values, ok := md[grpcGatewayContextMetadataHTTPPathPatternKey]
	if !ok {
		return "", false
	}
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// SetHTTPPathPatternFromContext sets the http path pattern in the context metadata.
func SetHTTPPathPatternFromContext(ctx context.Context, path string) context.Context {
	md := metadata.MD{grpcGatewayContextMetadataHTTPPathPatternKey: []string{path}} // /v1/example/login
	return metadata.NewOutgoingContext(ctx, md)
}

func withHTTPPatternAnnotation() runtime.ServeMuxOption {
	return runtime.WithMetadata(func(ctx context.Context, r *http.Request) metadata.MD {
		md := make(map[string]string)
		if pattern, ok := runtime.HTTPPathPattern(ctx); ok {
			md[grpcGatewayContextMetadataHTTPPathPatternKey] = pattern // /v1/example/login
		}
		return metadata.New(md)
	})
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

func customMimeWrapper(routeToGatewayOptions map[string]*grpcpb.GatewayOptions, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayOptions, ok := routeToGatewayOptions[r.URL.Path]
		if ok && gatewayOptions.CustomMime != "" {
			r.Header.Set("Content-Type", gatewayOptions.CustomMime)
		}
		h.ServeHTTP(w, r)
	})
}

func preflightHandler(w http.ResponseWriter, r *http.Request) {
	headers := []string{"Content-Type", "Accept", "Access-Control-Allow-Credentials"}
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

func withCustomMarshaler() runtime.ServeMuxOption {
	return runtime.WithMarshalerOption(runtime.MIMEWildcard, grpcGatewayMarshalerOptions)
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
	return runtime.DecoderFunc(func(v interface{}) error {
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
