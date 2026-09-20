package grpcwebproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
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
	"github.com/malonaz/core/go/lifecycle"
	"github.com/malonaz/core/go/prometheus"
)

type Opts struct {
	Port                int                    `long:"port" env:"PORT" description:"Port to serve gRPC-web proxy on." default:"8443"`
	Host                string                 `long:"host" env:"HOST" description:"Host to bind to." default:"0.0.0.0"`
	GracefulStopTimeout int                    `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"Seconds to wait for graceful stop." default:"30"`
	WriteTimeout        int                    `long:"write-timeout" env:"WRITE_TIMEOUT" description:"HTTP write timeout in seconds. 0 means no timeout." default:"0"`
	ReadTimeout         int                    `long:"read-timeout" env:"READ_TIMEOUT" description:"HTTP read timeout in seconds. 0 means no timeout." default:"0"`
	AllowAllOrigins     bool                   `long:"allow-all-origins" env:"ALLOW_ALL_ORIGINS" description:"Allow requests from any origin."`
	AllowedOrigins      []string               `long:"allowed-origins" env:"ALLOWED_ORIGINS" description:"Comma-separated list of allowed origins."`
	AllowedHeaders      []string               `long:"allowed-headers" env:"ALLOWED_HEADERS" description:"Headers allowed to propagate to backend."`
	Backend             *commongrpc.ClientOpts `group:"Backend" namespace:"backend" env-namespace:"BACKEND"`
}

type Server struct {
	log            *slog.Logger
	opts           *Opts
	certsOpts      *certs.Opts
	prometheusOpts *prometheus.Opts

	httpServer  *http.Server
	grpcServer  *grpc.Server
	lifecycle   lifecycle.State
	backendConn atomic.Pointer[grpc.ClientConn]
}

func NewServer(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts) *Server {
	s := &Server{
		opts:           opts,
		certsOpts:      certsOpts,
		prometheusOpts: prometheusOpts,
	}
	s.grpcServer = s.buildGRPCProxyServer()
	s.httpServer = &http.Server{
		Handler:      s.wrapWithGRPCWeb(),
		WriteTimeout: time.Duration(opts.WriteTimeout) * time.Second,
		ReadTimeout:  time.Duration(opts.ReadTimeout) * time.Second,
	}
	return s.WithLogger(slog.Default())
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger.WithGroup("grpcwebproxy").With(
		"port", s.opts.Port, "host", s.opts.Host,
		"backend_host", s.opts.Backend.Host, "backend_port", s.opts.Backend.Port,
	)
	return s
}

// Listen dials the backend and binds the proxy's address; Serve does so itself when not already bound.
func (s *Server) Listen(ctx context.Context) error {
	_, err := s.listen()
	return err
}

func (s *Server) listen() (net.Listener, error) {
	return s.lifecycle.Listen(func() (net.Listener, error) {
		if s.opts.AllowAllOrigins && len(s.opts.AllowedOrigins) > 0 {
			return nil, fmt.Errorf("ambiguous config: set either allow_all_origins or allowed_origins, not both")
		}
		backendConn, err := s.dialBackend()
		if err != nil {
			return nil, fmt.Errorf("dialing backend: %w", err)
		}
		listener, err := s.buildListener()
		if err != nil {
			backendConn.Close()
			return nil, err
		}
		s.backendConn.Store(backendConn)
		return listener, nil
	})
}

func (s *Server) closeBackend() {
	if backendConn := s.backendConn.Load(); backendConn != nil {
		backendConn.Close()
	}
}

// done completes a stop: releases an unadopted listener and closes the backend connection.
func (s *Server) done() {
	s.lifecycle.Done()
	s.closeBackend()
}

func (s *Server) Serve(ctx context.Context) error {
	listener, err := s.listen()
	if err != nil {
		return err
	}
	defer s.closeBackend()
	s.log.InfoContext(ctx, "serving")
	// Stopped before serving: a clean exit, like a stop during Serve.
	if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Stop closes the proxy immediately. A no-op once stopped.
func (s *Server) Stop() error {
	if !s.lifecycle.Stop() {
		return nil
	}
	defer s.done()
	s.log.Warn("stopping")
	s.httpServer.Close()
	s.grpcServer.Stop()
	return nil
}

// GracefulStop stops the proxy, waiting for in-flight requests up to the graceful stop timeout.
// A no-op once a stop has been requested.
func (s *Server) GracefulStop() error {
	if !s.lifecycle.GracefulStop() {
		return nil
	}
	defer s.done()
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	s.log.Info("gracefully stopping", "grace_period", duration)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.log.Warn("graceful shutdown failed, forcing", "error", err)
		s.httpServer.Close()
	}
	s.grpcServer.GracefulStop()
	s.log.Info("stopped gracefully")
	return nil
}

func (s *Server) HealthCheckFn() health.Check {
	return func(ctx context.Context) error {
		backendConn := s.backendConn.Load()
		if backendConn == nil {
			return fmt.Errorf("backend connection not established")
		}
		state := backendConn.GetState()
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
		if userAgents := mdCopy.Get("user-agent"); len(userAgents) > 0 {
			mdCopy.Set("x-forwarded-user-agent", userAgents...)
		}
		delete(mdCopy, "user-agent")
		delete(mdCopy, "connection")
		outCtx = metadata.NewOutgoingContext(outCtx, mdCopy)
		return outCtx, s.backendConn.Load(), nil
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
