package grpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"buf.build/go/protovalidate"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Enable compression.
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/malonaz/core/go/certs"
	grpc_interceptor "github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/prometheus"
)

const (
	// maximum message size for the server (20 MB)
	MaximumMessageSize = 20 * 1024 * 1024
)

var (
	prometheusDefaultHistogramBuckets = []float64{
		0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120,
	}

	serverKeepAliveEnforcementPolicy = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	serverKeepAliveParameters = keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY.
		MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY.
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections.
		Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active.
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead.
	}
)

// ServerOptions holds config for a server.
type ServerOptions struct {
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
	GRPCOptions        []grpc.ServerOption
}

// Server is a gRPC server.
type Server struct {
	opts                    *Opts
	prometheusOpts          *prometheus.Opts
	register                func(*Server)
	Raw                     *grpc.Server
	prometheusServerMetrics *grpc_prometheus.ServerMetrics
	healthServer            *health.GRPCServer

	// The **first** interceptor is the **outermost** (executes first on request, last on response).
	// Order of interceptors is [PRE_OPTIONS_DEFAULT, OPTIONS, POST_OPTIONS_DEFAULT].
	preUnaryInterceptors   []grpc.UnaryServerInterceptor
	unaryInterceptors      []grpc.UnaryServerInterceptor
	postUnaryInterceptors  []grpc.UnaryServerInterceptor
	preStreamInterceptors  []grpc.StreamServerInterceptor
	streamInterceptors     []grpc.StreamServerInterceptor
	postStreamInterceptors []grpc.StreamServerInterceptor

	options []grpc.ServerOption
}

// NewServer creates and returns a new Server.
func NewServer(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, register func(*Server)) (*Server, error) {
	server := &Server{
		opts:           opts,
		prometheusOpts: prometheusOpts,
		register:       register,
		healthServer:   health.NewGRPCServer(opts.Health),
	}

	// Default options.
	server.options = append(server.options, grpc.MaxRecvMsgSize(MaximumMessageSize), grpc.MaxSendMsgSize(MaximumMessageSize))
	if !opts.DisableTLS {
		tlsConfig, err := certsOpts.ServerTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading TLS config: %w", err)
		}
		server.options = append(server.options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Warningf("Starting gRPC server without TLS")
	}

	// Instantiate prometheus interceptors if relevant.
	var prometheusUnaryInterceptor grpc.UnaryServerInterceptor
	var prometheusStreamInterceptor grpc.StreamServerInterceptor
	if prometheusOpts.Enabled() {
		metrics := grpc_prometheus.NewServerMetrics(
			grpc_prometheus.WithServerHandlingTimeHistogram(
				grpc_prometheus.WithHistogramBuckets(prometheusDefaultHistogramBuckets),
			),
		)
		server.prometheusServerMetrics = metrics
		prometheusUnaryInterceptor = metrics.UnaryServerInterceptor()
		prometheusStreamInterceptor = metrics.StreamServerInterceptor()
	}

	// Instantiate validator.
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("instantiating proto validator: %w", err)
	}

	// PRE (1): Panic interceptor. We *never* want to panic.
	server.preUnaryInterceptors = append(
		server.preUnaryInterceptors, grpc_recovery.UnaryServerInterceptor(),
	)
	server.preStreamInterceptors = append(
		server.preStreamInterceptors, grpc_recovery.StreamServerInterceptor(),
	)
	// PRE (2): Prometheus first.
	if prometheusOpts.Enabled() {
		server.preUnaryInterceptors = append(server.preUnaryInterceptors, prometheusUnaryInterceptor)
		server.preStreamInterceptors = append(server.preStreamInterceptors, prometheusStreamInterceptor)
	}
	// PRE (3): Context tags: allows downstream interceptors to pass values back down to the logging interceptor.
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerContextTagsInitializer())
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerContextTagsInitializer())
	// PRE (4): Context propagator: propagates incoming.metadata headers to outgoing.metadata headers
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerHeaderPropagation())
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerHeaderPropagation())
	// PRE (5): Logging interceptor.
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerLogging(log))
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerLogging(log))

	// PRE (6): Trailer propagator interceptor.
	server.preUnaryInterceptors = append(
		server.preUnaryInterceptors, grpc_interceptor.UnaryServerTrailerPropagation(),
	)
	server.preStreamInterceptors = append(
		server.preStreamInterceptors, grpc_interceptor.StreamServerTrailerPropagation(),
	)

	// POST (1): Proto validator interceptor. (Last before it goes on the wire).
	server.postUnaryInterceptors = append(
		server.postUnaryInterceptors,
		grpc_interceptor.UnaryServerValidate(validator),
		grpc_interceptor.UnaryServerFieldMask(),
	)
	server.postStreamInterceptors = append(
		server.postStreamInterceptors, grpc_interceptor.StreamServerValidate(validator),
	)
	return server, nil
}

func (s *Server) GetHealthServer() *health.GRPCServer {
	return s.healthServer
}

// WithOptions adds options to this gRPC server.
func (s *Server) WithOptions(options ...grpc.ServerOption) *Server {
	s.options = append(s.options, options...)
	return s
}

// WithUnaryInterceptors adds interceptors to this gRPC server.
// These interceptors are added *AFTER* the default pre interceptors and *BEFORE* the default post interceptors.
func (s *Server) WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) *Server {
	s.unaryInterceptors = append(s.unaryInterceptors, interceptors...)
	return s
}

// WithStreamInterceptors adds interceptors to this gRPC server.
// These interceptors are added *AFTER* the default pre interceptors and *BEFORE* the default post interceptors.
func (s *Server) WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) *Server {
	s.streamInterceptors = append(s.streamInterceptors, interceptors...)
	return s
}

func (s *Server) Stop() error {
	log.Warningf("stopping server")
	s.Raw.Stop()
	s.healthServer.Shutdown()
	return nil
}

func (s *Server) GracefulStop() error {
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	ch := make(chan struct{})
	go func() {
		log.Infof("attempting to gracefully stop server, with a grace period of %s", duration)
		s.Raw.GracefulStop()
		log.Info("server stopped gracefully")
		ch <- struct{}{}
	}()
	select {
	case <-time.After(duration):
		log.Infof("grace period exhausted")
		s.Stop()
	case <-ch:
	}
	s.healthServer.Shutdown()
	return nil
}

// Serve instantiates the gRPC server and blocks forever.
func (s *Server) Serve(ctx context.Context) error {
	unaryInterceptors := append(s.preUnaryInterceptors, s.unaryInterceptors...)
	unaryInterceptors = append(unaryInterceptors, s.postUnaryInterceptors...)
	streamInterceptors := append(s.preStreamInterceptors, s.streamInterceptors...)
	streamInterceptors = append(streamInterceptors, s.postStreamInterceptors...)
	// Chain interceptors.
	if len(unaryInterceptors) > 0 {
		s.options = append(s.options, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		s.options = append(s.options, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	// Create listener based on network type
	var listener net.Listener
	var err error
	if s.opts.useSocket() {
		defer os.Remove(s.opts.SocketPath)
		// Clean up existing socket file if it exists
		if err := os.RemoveAll(s.opts.SocketPath); err != nil {
			return fmt.Errorf("removing existing socket [%s]: %w", s.opts.SocketPath, err)
		}
		// Ensure directory exists
		dir := filepath.Dir(s.opts.SocketPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating socket  [%s]: %w", s.opts.SocketPath, err)
		}
		listener, err = net.Listen("unix", s.opts.SocketPath)
		if err != nil {
			return fmt.Errorf("listening on socket [%s]: %w", s.opts.SocketPath, err)
		}
		// Set appropriate permissions
		if err := os.Chmod(s.opts.SocketPath, 0666); err != nil {
			return fmt.Errorf("setting socket os permissions [%s]: %w", s.opts.SocketPath, err)
		}
		log.Infof("Serving gRPC on Unix socket [%s]", s.opts.SocketPath)
	} else {
		// Connect.
		listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.opts.Port))
		if err != nil {
			return fmt.Errorf("listening on port [%d]: %w", s.opts.Port, err)
		}
		log.Infof("Serving gRPC on port [:%d]", s.opts.Port)
	}
	defer listener.Close()

	s.Raw = grpc.NewServer(s.options...)
	s.register(s)
	grpc_health_v1.RegisterHealthServer(s.Raw, s.healthServer)
	s.healthServer.Start(ctx)

	if s.prometheusOpts.Enabled() {
		s.prometheusServerMetrics.InitializeMetrics(s.Raw)
		prom.DefaultRegisterer.MustRegister(s.prometheusServerMetrics)
	}
	if err := s.Raw.Serve(listener); err != nil {
		return fmt.Errorf("server exited unexpectedly: %w", err)
	}
	return nil
}
