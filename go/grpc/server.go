package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"buf.build/go/protovalidate"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Enable compression.
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	grpc_reflection_v1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbreflection"
	"github.com/malonaz/core/go/prometheus"
)

const (
	// maximum message size for the server (20 MB)
	MaximumMessageSize = 20 * 1024 * 1024
)

var (
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
	log            *slog.Logger
	opts           *Opts
	certsOpts      *certs.Opts
	prometheusOpts *prometheus.Opts
	register       func(*Server)
	Raw            *grpc.Server
	healthServer   *health.GRPCServer

	fdsBytes []byte

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

func (s *Server) WithFileDescriptorSet(bytes []byte) *Server {
	s.fdsBytes = bytes
	return s
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger
	return s
}

// NewServer creates and returns a new Server.
func NewServer(opts *Opts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, register func(*Server)) *Server {
	return &Server{
		log:            slog.Default(),
		opts:           opts,
		certsOpts:      certsOpts,
		prometheusOpts: prometheusOpts,
		register:       register,
		healthServer:   health.NewGRPCServer(opts.Health),
	}
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
	s.log.Warn("stopping")
	if s.Raw != nil {
		s.Raw.Stop()
	}
	s.healthServer.Shutdown()
	return nil
}

func (s *Server) GracefulStop() error {
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	ch := make(chan struct{})
	go func() {
		s.log.Info("gracefully stopping", "grace_period", duration)
		if s.Raw != nil {
			s.Raw.GracefulStop()
		}
		s.log.Info("stopped gracefully")
		ch <- struct{}{}
	}()
	select {
	case <-time.After(duration):
		s.log.Info("grace period exhausted")
		s.Stop()
	case <-ch:
	}
	s.healthServer.Shutdown()
	return nil
}

// Serve instantiates the gRPC server and blocks forever.
func (s *Server) Serve(ctx context.Context) error {
	s.log = s.log.WithGroup("grpc_server").With(
		"port", s.opts.Port, "host", s.opts.Host, "socket_path", s.opts.SocketPath,
		"disable_tls", s.opts.DisableTLS,
	)
	// Default options.
	s.options = append(s.options, grpc.MaxRecvMsgSize(MaximumMessageSize), grpc.MaxSendMsgSize(MaximumMessageSize))
	if !s.opts.DisableTLS {
		tlsConfig, err := s.certsOpts.ServerTLSConfig()
		if err != nil {
			return fmt.Errorf("loading TLS config: %w", err)
		}
		s.options = append(s.options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		s.log.WarnContext(ctx, "starting without TLS")
	}

	// Instantiate validator.
	validator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("instantiating proto validator: %w", err)
	}

	// PRE (1): Panic interceptor. We *never* want to panic.
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	s.preStreamInterceptors = append(s.preStreamInterceptors, grpc_recovery.StreamServerInterceptor())
	// PRE (2): Error debug info scrubber (acts on the response so needs to be placed early).
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerDebugInfoScrubber())
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerDebugInfoScrubber())
	// PRE (3): Prometheus first.
	if s.prometheusOpts.Enabled() {
		prometheusServerMetrics := getPrometheusServerMetrics()
		s.preUnaryInterceptors = append(s.preUnaryInterceptors, grpc_selector.UnaryServerInterceptor(prometheusServerMetrics.UnaryServerInterceptor(), middleware.AllButHealth))
		s.preStreamInterceptors = append(s.preStreamInterceptors, grpc_selector.StreamServerInterceptor(prometheusServerMetrics.StreamServerInterceptor(), middleware.AllButHealth))
	}
	// PRE (4): Context propagator: propagates incoming.metadata headers to outgoing.metadata headers
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerHeaderPropagation())
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerHeaderPropagation())
	// PRE (5): Trailer propagator interceptor.
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerTrailerPropagation())
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerTrailerPropagation())
	// PRE (6): Inject context tag: allows downstream components to inject log fields via the ctx for the logging interceptor to log.
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerLogContextTagInitializer())
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerLogContextTagInitializer())
	// PRE (7): Logging interceptor.
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerLogging(s.log))
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerLogging(s.log))
	// PRE (8): Error interceptor.
	s.preUnaryInterceptors = append(s.preUnaryInterceptors, middleware.UnaryServerErrorInfoInjector())
	s.preStreamInterceptors = append(s.preStreamInterceptors, middleware.StreamServerErrorInfoInjector())

	// POST (1): Proto validator interceptor. (Last before it goes on the wire).
	// We place this interceptor in the 'post' to allow authentication middleware to take precedence.
	s.postUnaryInterceptors = append(s.postUnaryInterceptors, middleware.UnaryServerValidate(validator))
	s.postStreamInterceptors = append(s.postStreamInterceptors, middleware.StreamServerValidate(validator))
	// POST (2): Canonicalize interceptor (After proto validate).
	s.postUnaryInterceptors = append(s.postUnaryInterceptors, middleware.UnaryServerCanonicalize())
	s.postStreamInterceptors = append(s.postStreamInterceptors, middleware.StreamServerCanonicalize())
	// POST (3): Field mask interceptor.
	s.postUnaryInterceptors = append(s.postUnaryInterceptors, middleware.UnaryServerFieldMask())
	s.postStreamInterceptors = append(s.postStreamInterceptors, middleware.StreamServerFieldMask())

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
	} else {
		// Connect.
		listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.opts.Port))
		if err != nil {
			return fmt.Errorf("listening on port [%d]: %w", s.opts.Port, err)
		}
	}
	defer listener.Close()

	s.Raw = grpc.NewServer(s.options...)
	s.register(s)
	grpc_health_v1.RegisterHealthServer(s.Raw, s.healthServer)
	s.healthServer.Start(ctx)

	if s.opts.EnableReflection {
		if s.fdsBytes != nil {
			var fds descriptorpb.FileDescriptorSet
			if err := pbutil.Unmarshal(s.fdsBytes, &fds); err != nil {
				return err
			}
			// Convert FileDescriptorSet to a resolver
			files, err := protodesc.NewFiles(&fds)
			if err != nil {
				return fmt.Errorf("building file descriptor registry: %w", err)
			}
			types, err := pbreflection.NewTypesFromFiles(files)
			if err != nil {
				return fmt.Errorf("new types from files: %w", err)
			}
			reflectionServerOptions := reflection.ServerOptions{
				Services:           s.Raw,
				ExtensionResolver:  types,
				DescriptorResolver: files,
			}
			reflectionServer := reflection.NewServerV1(reflectionServerOptions)
			grpc_reflection_v1.RegisterServerReflectionServer(s.Raw, reflectionServer)

		} else {
			reflection.Register(s.Raw)
		}
		s.GetHealthServer().RegisterService(grpc_reflection_v1.ServerReflection_ServiceDesc.ServiceName)
		s.log.InfoContext(ctx, "gRPC reflection enabled")
	}

	if s.prometheusOpts.Enabled() {
		getPrometheusServerMetrics().InitializeMetrics(s.Raw)
	}

	s.log.InfoContext(ctx, "serving")
	if err := s.Raw.Serve(listener); err != nil {
		return fmt.Errorf("server exited unexpectedly: %w", err)
	}
	return nil
}
