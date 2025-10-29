package grpc

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Enable compression.
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/malonaz/core/go/certs"
	grpc_interceptor "github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/prometheus"
)

const (
	// maximum message size for the server (20 MB)
	MaximumMessageSize         = 20 * 1024 * 1024
	gracefulStopTimeoutSeconds = 30
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

	healthCheck         health.Check
	healthCheckErr      error
	healthCheckErrMutex sync.RWMutex

	// Order of interceptors is [PRE_DEFAULT, PRE_OPTIONS, POST_OPTIONS, POST_DEFAULT].
	preUnaryInterceptors   []grpc.UnaryServerInterceptor
	postUnaryInterceptors  []grpc.UnaryServerInterceptor
	preStreamInterceptors  []grpc.StreamServerInterceptor
	postStreamInterceptors []grpc.StreamServerInterceptor

	options []grpc.ServerOption
}

// NewServer creates and returns a new Server.
func NewServer(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, register func(*Server)) *Server {
	server := &Server{
		opts:           opts,
		prometheusOpts: prometheusOpts,
		register:       register,
	}

	// Default options.
	server.options = append(server.options, grpc.MaxRecvMsgSize(MaximumMessageSize), grpc.MaxSendMsgSize(MaximumMessageSize))
	if !opts.DisableTLS {
		tlsConfig, err := certsOpts.ServerTLSConfig()
		if err != nil {
			log.Panicf("Could not load server TLS config: %v", err)
		}
		server.options = append(server.options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Warningf("Starting gRPC server without TLS")
	}

	// Instantiate prometheus interceptors if relevant.
	var prometheusUnaryInterceptor grpc.UnaryServerInterceptor
	var prometheusStreamInterceptor grpc.StreamServerInterceptor
	if !prometheusOpts.Disable {
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
		log.Panicf("could not instantiate proto validator")
	}

	// PRE (1): Prometheus first.
	if !prometheusOpts.Disable {
		server.preUnaryInterceptors = append(server.preUnaryInterceptors, prometheusUnaryInterceptor)
		server.preStreamInterceptors = append(server.preStreamInterceptors, prometheusStreamInterceptor)
	}
	// PRE (2): Context tags (allows downstream interceptors to pass values back down to the logging interceptor.
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerContextTagsInitializer())
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerContextTagsInitializer())
	// PRE (3): Context propagator (it overwrites and outgoing context written prior so must be placed first.
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerContextPropagation())
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerContextPropagation())
	// PRE (4): Logging interceptor.
	server.preUnaryInterceptors = append(server.preUnaryInterceptors, grpc_interceptor.UnaryServerLogging(log))
	server.preStreamInterceptors = append(server.preStreamInterceptors, grpc_interceptor.StreamServerLogging(log))
	// PRE (5): Panic interceptor.
	server.preUnaryInterceptors = append(
		server.preUnaryInterceptors, grpc_recovery.UnaryServerInterceptor(),
	)
	server.preStreamInterceptors = append(
		server.preStreamInterceptors, grpc_recovery.StreamServerInterceptor(),
	)
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
	return server
}

// WithHealthCheck adds grpc health check capabilitites to the server.
func (s *Server) WithHealthCheck(healthCheck health.Check) *Server {
	s.healthCheck = healthCheck
	return s
}

// WithOptions adds options to this gRPC server.
func (s *Server) WithOptions(options ...grpc.ServerOption) *Server {
	s.options = append(s.options, options...)
	return s
}

// WithPreUnaryInterceptors adds interceptors to this gRPC server.
// These interceptors are added *AFTER* the default pre interceptors.
func (s *Server) WithPreUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) *Server {
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, interceptors...)
	return s
}

// WithPostUnaryInterceptors adds interceptors to this gRPC server.
// These interceptors are added *BEFORE* the default post interceptors, in the order they are given.
func (s *Server) WithPostUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) *Server {
	s.postUnaryInterceptors = append(interceptors, s.postUnaryInterceptors...)
	return s
}

// WithPreStreamInterceptors adds interceptors to this gRPC server.
// These interceptors are added *AFTER* the default pre interceptors.
func (s *Server) WithPreStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) *Server {
	s.preStreamInterceptors = append(s.preStreamInterceptors, interceptors...)
	return s
}

// WithPostStreamInterceptors adds interceptors to this gRPC server.
// These interceptors are added *BEFORE* the default post interceptors, in the order they are given.
func (s *Server) WithPostStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) *Server {
	s.postStreamInterceptors = append(interceptors, s.postStreamInterceptors...)
	return s
}

func (s *Server) gracefulStop(server *grpc.Server) {
	ch := make(chan struct{})
	go func() {
		log.Infof("attempting to gracefully stop server, with a grace period of %d seconds", gracefulStopTimeoutSeconds)
		server.GracefulStop()
		log.Info("server stopped")
		ch <- struct{}{}
	}()
	select {
	case <-time.After(time.Duration(gracefulStopTimeoutSeconds) * time.Second):
		log.Infof("grace period exhausted, stopping server")
		server.Stop()
	case <-ch:
	}
}

// Serve instantiates the gRPC server and blocks forever.
func (s *Server) Serve() {
	chainUnaryInterceptorOption := grpc.ChainUnaryInterceptor(
		append(s.preUnaryInterceptors, s.postUnaryInterceptors...)...,
	)
	chainStreamInterceptorOption := grpc.ChainStreamInterceptor(
		append(s.preStreamInterceptors, s.postStreamInterceptors...)...,
	)
	s.options = append(s.options, chainUnaryInterceptorOption, chainStreamInterceptorOption)

	// Connect.
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(s.opts.Port))
	if err != nil {
		log.Panicf("Failed to listen on port [%d]: %v", s.opts.Port, err)
	}
	defer listener.Close()
	log.Infof("Serving gRPC on port [:%d]", s.opts.Port)

	s.Raw = grpc.NewServer(s.options...)
	s.register(s)
	if s.healthCheck != nil {
		go s.assertHealthPeriodically()
		grpc_health_v1.RegisterHealthServer(s.Raw, s)
	}
	go handleSignals(func() { s.gracefulStop(s.Raw) }, s.Raw.Stop)
	if !s.prometheusOpts.Disable {
		s.prometheusServerMetrics.InitializeMetrics(s.Raw)
		prom.DefaultRegisterer.MustRegister(s.prometheusServerMetrics)
	}
	if err := s.Raw.Serve(listener); err != nil {
		log.Panicf("gRPC server exited with error: %v", err)
	}
}

func (s *Server) assertHealthPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		err := s.healthCheck(context.Background())
		s.healthCheckErrMutex.Lock()
		s.healthCheckErr = err
		s.healthCheckErrMutex.Unlock()
	}
}

// Check implements the grpc health v1 interface.
func (s *Server) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	s.healthCheckErrMutex.RLock()
	defer s.healthCheckErrMutex.RUnlock()

	status := grpc_health_v1.HealthCheckResponse_SERVING
	if s.healthCheckErr != nil {
		status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}

	return &grpc_health_v1.HealthCheckResponse{Status: status}, nil
}

// Watch implements the grpc health v1 interface.
func (s *Server) Watch(in *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

// List implements the grpc health v1 interface.
func (s *Server) List(ctx context.Context, in *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method List not implemented")
}
