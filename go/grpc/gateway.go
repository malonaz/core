package grpc

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"go/certs"
	"go/prometheus"
)

const grpcGatewayContextMetadataHTTPPathPatternKey = "http-path-pattern"

// RegisterHandler is syntactice sugar for a gRPC gateway handler.
type RegisterHandler = func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error)

// Gateway is a gRPC gateway.
type Gateway struct {
	opts             GatewayOpts
	registerHandlers []RegisterHandler

	// HTTP.
	options []runtime.ServeMuxOption

	// GRPC Client.
	dialOptions []grpc.DialOption
	// First interceptor is executed first.
	unaryInterceptors  []grpc.UnaryClientInterceptor
	streamInterceptors []grpc.StreamClientInterceptor
}

// NewGateway creates and returns a new Gateway.
func NewGateway(opts GatewayOpts, certsOpts certs.Opts, prometheusOpts prometheus.Opts, registerHandlers []RegisterHandler) *Gateway {
	gateway := &Gateway{
		opts:             opts,
		registerHandlers: registerHandlers,
		options: []runtime.ServeMuxOption{
			// Some default options.
			runtime.WithOutgoingHeaderMatcher(outgoingHeaderMatcher),
			runtime.WithForwardResponseOption(GatewayCookie{}.forwardOutOption),
			runtime.WithMetadata(GatewayCookie{}.forwardInOption),
			withCustomMarshaler(),
			withHTTPPatternAnnotation(),
		},
	}

	// Default dial options.
	gateway.dialOptions = append(gateway.dialOptions, withDNSBalancer()) // Currently makes no sense as we are dialing to localhost.
	if opts.GRPC.DisableTLS {
		log.Warningf("Starting gRPC client using insecure gRPC dial")
		gateway.dialOptions = append(gateway.dialOptions, grpc.WithInsecure())
	} else {
		tlsConfig, err := certsOpts.ClientTLSConfig()
		if err != nil {
			log.Panicf("Could not load client TLS config: %v", err)
		}
		gateway.dialOptions = append(gateway.dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Default interceptors.
	gateway.unaryInterceptors = append(gateway.unaryInterceptors, withUnaryRetry())
	if !prometheusOpts.Disable {
		gateway.unaryInterceptors = append(gateway.unaryInterceptors, grpc_prometheus.UnaryClientInterceptor)
		gateway.streamInterceptors = append(gateway.streamInterceptors, grpc_prometheus.StreamClientInterceptor)
	}
	return gateway
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
func (g *Gateway) Serve() {
	// Chain interceptors.
	if len(g.unaryInterceptors) > 0 {
		g.dialOptions = append(g.dialOptions, grpc.WithChainUnaryInterceptor(g.unaryInterceptors...))
	}
	if len(g.streamInterceptors) > 0 {
		g.dialOptions = append(g.dialOptions, grpc.WithChainStreamInterceptor(g.streamInterceptors...))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := runtime.NewServeMux(g.options...)
	endpoint := fmt.Sprintf(":%d", g.opts.GRPC.Port)
	for _, registerHandler := range g.registerHandlers {
		if err := registerHandler(ctx, mux, endpoint, g.dialOptions); err != nil {
			log.Panicf("Could not register handler: %v", err)
		}
	}
	go handleSignals(cancel)
	log.Infof("Connected to gRPC server on [%s]", endpoint)

	url := fmt.Sprintf(":%d", g.opts.Port)
	httpServer := http.Server{Addr: url, Handler: customMimeWrapper(allowCORS(mux))}
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Panicf("Gateway server exited unexpectedly: %v", err)
		}
	}()
	log.Infof("Serving gRPC Gateway on port [:%d]", g.opts.Port)

	<-ctx.Done()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Panicf("Could not shutdown http server: %v", err)
	}
}

// /////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////// VARIOUS GATEWAY OPTIONS BELOW //////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
var allowedHeaders = map[string]struct{}{}

func outgoingHeaderMatcher(key string) (string, bool) {
	if _, ok := allowedHeaders[key]; ok {
		// Note that this is what the default behaviour of the gRPC library does (where all headers are allowed).
		return fmt.Sprintf("%s%s", runtime.MetadataHeaderPrefix, key), true
	}
	return key, false
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

func customMimeWrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "webhook") {
			r.Header.Set("Content-Type", "application/raw-webhook")
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
	return runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{
			UseEnumNumbers:  true,
			EmitUnpopulated: true,
		},
		UnmarshalOptions: protojson.UnmarshalOptions{},
	})
}
