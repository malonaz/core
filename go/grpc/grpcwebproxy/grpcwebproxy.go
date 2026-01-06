package grpcwebproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/mwitkow/go-conntrack"
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/malonaz/core/go/certs"
	commongrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/prometheus"
)

type Opts struct {
	Port                int              `long:"port" env:"PORT" description:"Port to serve gRPC-web proxy on." default:"8443"`
	Host                string           `long:"host" env:"HOST" description:"Host to bind to." default:"0.0.0.0"`
	GracefulStopTimeout int              `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"Seconds to wait for graceful stop." default:"30"`
	AllowAllOrigins     bool             `long:"allow-all-origins" env:"ALLOW_ALL_ORIGINS" description:"Allow requests from any origin."`
	AllowedOrigins      []string         `long:"allowed-origins" env:"ALLOWED_ORIGINS" description:"Comma-separated list of allowed origins."`
	AllowedHeaders      []string         `long:"allowed-headers" env:"ALLOWED_HEADERS" description:"Headers allowed to propagate to backend."`
	Backend             *commongrpc.Opts `group:"Backend" namespace:"backend" env-namespace:"BACKEND"`
}

type Server struct {
	log            *slog.Logger
	opts           *Opts
	certsOpts      *certs.Opts
	prometheusOpts *prometheus.Opts

	httpServer  *http.Server
	grpcServer  *grpc.Server
	backendConn *grpc.ClientConn
}

func NewServer(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts) *Server {
	return &Server{
		log:            slog.Default(),
		opts:           opts,
		certsOpts:      certsOpts,
		prometheusOpts: prometheusOpts,
	}
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger
	return s
}

func (s *Server) Serve(ctx context.Context) error {
	s.log = s.log.WithGroup("grpcwebproxy").With(
		"port", s.opts.Port, "host", s.opts.Host,
		"backend_host", s.opts.Backend.Host, "backend_port", s.opts.Backend.Port,
	)

	if s.opts.AllowAllOrigins && len(s.opts.AllowedOrigins) > 0 {
		return fmt.Errorf("ambiguous config: set either allow_all_origins or allowed_origins, not both")
	}

	var err error
	s.backendConn, err = s.dialBackend()
	if err != nil {
		return fmt.Errorf("dialing backend: %w", err)
	}
	defer s.backendConn.Close()

	s.grpcServer = s.buildGRPCProxyServer()
	wrappedGrpc := s.wrapWithGRPCWeb()

	listener, err := s.buildListener()
	if err != nil {
		return err
	}
	defer listener.Close()

	s.httpServer = &http.Server{
		Handler:      wrappedGrpc,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}

	s.log.InfoContext(ctx, "serving")
	if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

func (s *Server) Stop() error {
	s.log.Warn("stopping")
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	return nil
}

func (s *Server) GracefulStop() error {
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	s.log.Info("gracefully stopping", "grace_period", duration)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server shutdown error", "error", err)
		}
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.log.Info("stopped gracefully")
	return nil
}

func (s *Server) HealthCheckFn() health.Check {
	return func(ctx context.Context) error {
		if s.backendConn == nil {
			return fmt.Errorf("backend connection not established")
		}
		state := s.backendConn.GetState()
		if state.String() == "TRANSIENT_FAILURE" || state.String() == "SHUTDOWN" {
			return fmt.Errorf("backend connection state: %s", state)
		}
		return nil
	}
}

func (s *Server) dialBackend() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		commongrpc.WithDNSBalancer(),
		grpc.WithCodec(proxy.Codec()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(commongrpc.MaximumMessageSize)),
	}

	if s.opts.Backend.DisableTLS {
		opts = append(opts, grpc.WithInsecure())
	} else {
		tlsConfig, err := s.certsOpts.ClientTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading client TLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	endpoint := s.opts.Backend.Endpoint()
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", endpoint, err)
	}
	s.log.Info("connected to backend", "endpoint", endpoint)
	return conn, nil
}

func (s *Server) buildGRPCProxyServer() *grpc.Server {
	director := func(ctx context.Context, fullMethodName string) (context.Context, grpc.ClientConnInterface, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		outCtx, _ := context.WithCancel(ctx)
		mdCopy := md.Copy()
		delete(mdCopy, "user-agent")
		delete(mdCopy, "connection")
		outCtx = metadata.NewOutgoingContext(outCtx, mdCopy)
		return outCtx, s.backendConn, nil
	}

	serverOpts := []grpc.ServerOption{
		grpc.CustomCodec(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		grpc.MaxRecvMsgSize(commongrpc.MaximumMessageSize),
		grpc.MaxSendMsgSize(commongrpc.MaximumMessageSize),
	}

	if s.prometheusOpts.Enabled() {
		metrics := grpc_prometheus.NewServerMetrics()
		serverOpts = append(serverOpts,
			grpc_middleware.WithUnaryServerChain(metrics.UnaryServerInterceptor()),
			grpc_middleware.WithStreamServerChain(metrics.StreamServerInterceptor()),
		)
	}

	return grpc.NewServer(serverOpts...)
}

func (s *Server) wrapWithGRPCWeb() http.Handler {
	allowedOrigins := make(map[string]struct{})
	for _, o := range s.opts.AllowedOrigins {
		allowedOrigins[o] = struct{}{}
	}

	originFunc := func(origin string) bool {
		if s.opts.AllowAllOrigins {
			return true
		}
		_, ok := allowedOrigins[origin]
		return ok
	}

	options := []grpcweb.Option{
		grpcweb.WithCorsForRegisteredEndpointsOnly(false),
		grpcweb.WithOriginFunc(originFunc),
	}

	if len(s.opts.AllowedHeaders) > 0 {
		options = append(options, grpcweb.WithAllowedRequestHeaders(s.opts.AllowedHeaders))
	}

	return grpcweb.WrapServer(s.grpcServer, options...)
}

func (s *Server) buildListener() (net.Listener, error) {
	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", addr, err)
	}

	listener = conntrack.NewListener(listener,
		conntrack.TrackWithName("grpcwebproxy"),
		conntrack.TrackWithTcpKeepAlive(20*time.Second),
		conntrack.TrackWithTracing(),
	)

	if s.certsOpts != nil && !s.opts.Backend.DisableTLS {
		tlsConfig, err := s.certsOpts.ServerTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading server TLS config: %w", err)
		}
		tlsConfig.MinVersion = tls.VersionTLS12
		listener = tls.NewListener(listener, tlsConfig)
		s.log.Info("TLS enabled")
	}

	return listener, nil
}
