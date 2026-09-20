// go/grpc/server.go
package grpc

import (
	"context"
	"errors"
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
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	grpc_reflection_v1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/lifecycle"
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

// ServerOptions holds what a caller adds to a server: interceptors between the default pre and post
// chains (the first is the outermost), extra gRPC options, and a FileDescriptorSet for reflection.
type ServerOptions struct {
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
	GRPCOptions        []grpc.ServerOption
	FileDescriptorSet  []byte
}

// Server is a gRPC server, fully built by NewServer so a stop reaches it whether or not Serve has run.
type Server struct {
	name         string
	log          *slog.Logger
	opts         *ServerOpts
	healthServer *health.GRPCServer
	Raw          *grpc.Server
	lifecycle    lifecycle.State
}

// NewServer builds the server and lets register add services to Raw.
func NewServer(opts *ServerOpts, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts, name string, options ServerOptions, register func(*Server)) (*Server, error) {
	s := &Server{
		name:         name,
		opts:         opts,
		healthServer: health.NewGRPCServer(opts.Health, name),
	}
	s.WithLogger(slog.Default())

	grpcOptions := []grpc.ServerOption{grpc.MaxRecvMsgSize(MaximumMessageSize), grpc.MaxSendMsgSize(MaximumMessageSize)}
	grpcOptions = append(grpcOptions, options.GRPCOptions...)
	if !opts.DisableTLS {
		tlsConfig, err := certsOpts.ServerTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("loading TLS config: %w", err)
		}
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		s.log.Warn("starting without TLS")
	}

	// Instantiate validator.
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("instantiating proto validator: %w", err)
	}

	var preUnaryInterceptors, postUnaryInterceptors []grpc.UnaryServerInterceptor
	var preStreamInterceptors, postStreamInterceptors []grpc.StreamServerInterceptor
	// PRE (1): Panic interceptor. We *never* want to panic.
	preUnaryInterceptors = append(preUnaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	preStreamInterceptors = append(preStreamInterceptors, grpc_recovery.StreamServerInterceptor())
	// PRE (2): Error debug info scrubber (acts on the response so needs to be placed early).
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerDebugInfoScrubber())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerDebugInfoScrubber())
	// PRE (3): Method descriptor resolver. Makes the method descriptor available to all downstream interceptors.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerMethodDescriptor())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerMethodDescriptor())
	// PRE (4): Prometheus.
	if prometheusOpts.Enabled() {
		prometheusServerMetrics := getPrometheusServerMetrics()
		preUnaryInterceptors = append(preUnaryInterceptors, grpc_selector.UnaryServerInterceptor(prometheusServerMetrics.UnaryServerInterceptor(), middleware.AllButHealth))
		preStreamInterceptors = append(preStreamInterceptors, grpc_selector.StreamServerInterceptor(prometheusServerMetrics.StreamServerInterceptor(), middleware.AllButHealth))
	}
	// PRE (5): Context propagator: propagates incoming.metadata headers to outgoing.metadata headers
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerHeaderPropagation())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerHeaderPropagation())
	// PRE (6): Trailer propagator interceptor.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerTrailerPropagation())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerTrailerPropagation())
	// PRE (7): Inject context tag: allows downstream components to inject log fields via the ctx for the logging interceptor to log.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerLogContextTagInitializer())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerLogContextTagInitializer())
	// PRE (7.5): AIP logging: injects resource name fields from requests into log context.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerAIPLogging())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerAIPLogging())
	// PRE (8): Logging interceptor.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerLogging(s.log))
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerLogging(s.log))
	// PRE (9): Error interceptor.
	preUnaryInterceptors = append(preUnaryInterceptors, middleware.UnaryServerErrorInfoInjector())
	preStreamInterceptors = append(preStreamInterceptors, middleware.StreamServerErrorInfoInjector())

	// POST (1): Hook interceptor. Used in `Post` to allow authentication middleware to take precedence.
	postUnaryInterceptors = append(postUnaryInterceptors, middleware.UnaryServerHook())
	postStreamInterceptors = append(postStreamInterceptors, middleware.StreamServerHook())
	// POST (2): Canonicalize interceptor. Set after protovalidate.
	postUnaryInterceptors = append(postUnaryInterceptors, middleware.UnaryServerCanonicalize())
	postStreamInterceptors = append(postStreamInterceptors, middleware.StreamServerCanonicalize())
	// POST (3): Proto validator interceptor. Used in `Post` to allow authentication middleware to take precedence.
	postUnaryInterceptors = append(postUnaryInterceptors, middleware.UnaryServerValidate(validator))
	postStreamInterceptors = append(postStreamInterceptors, middleware.StreamServerValidate(validator))
	// POST (4): Field mask interceptor.
	postUnaryInterceptors = append(postUnaryInterceptors, middleware.UnaryServerFieldMask())
	postStreamInterceptors = append(postStreamInterceptors, middleware.StreamServerFieldMask())

	unaryInterceptors := append(preUnaryInterceptors, options.UnaryInterceptors...)
	unaryInterceptors = append(unaryInterceptors, postUnaryInterceptors...)
	streamInterceptors := append(preStreamInterceptors, options.StreamInterceptors...)
	streamInterceptors = append(streamInterceptors, postStreamInterceptors...)
	// Chain interceptors.
	if len(unaryInterceptors) > 0 {
		grpcOptions = append(grpcOptions, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		grpcOptions = append(grpcOptions, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	s.Raw = grpc.NewServer(grpcOptions...)
	register(s)
	grpc_health_v1.RegisterHealthServer(s.Raw, s.healthServer)

	if s.opts.EnableReflection {
		if options.FileDescriptorSet != nil {
			var fds descriptorpb.FileDescriptorSet
			if err := pbutil.Unmarshal(options.FileDescriptorSet, &fds); err != nil {
				return nil, err
			}
			// Convert FileDescriptorSet to a resolver
			files, err := protodesc.NewFiles(&fds)
			if err != nil {
				return nil, fmt.Errorf("building file descriptor registry: %w", err)
			}
			types, err := pbreflection.NewTypesFromFiles(files)
			if err != nil {
				return nil, fmt.Errorf("new types from files: %w", err)
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
		s.log.Info("gRPC reflection enabled")
	}

	if prometheusOpts.Enabled() {
		getPrometheusServerMetrics().InitializeMetrics(s.Raw)
	}
	return s, nil
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger.WithGroup("grpc_server").With(
		"name", s.name, "port", s.opts.Port, "socket_path",
		s.opts.SocketPath, "disable_tls", s.opts.DisableTLS,
	)
	return s
}

func (s *Server) GetHealthServer() *health.GRPCServer {
	return s.healthServer
}

// Stop closes all connections and listeners immediately. A no-op once stopped.
func (s *Server) Stop() error {
	if !s.lifecycle.Stop() {
		return nil
	}
	defer s.lifecycle.Done()
	s.log.Warn("stopping")
	s.healthServer.Shutdown()
	s.Raw.Stop()
	return nil
}

// GracefulStop stops accepting new RPCs and waits for pending ones up to the graceful stop timeout,
// after which it stops forcibly. A no-op once a stop has been requested.
func (s *Server) GracefulStop() error {
	if !s.lifecycle.GracefulStop() {
		return nil
	}
	defer s.lifecycle.Done()
	// NOT_SERVING first, so load balancers polling health stop routing to us while we drain.
	s.healthServer.Shutdown()
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	s.log.Info("gracefully stopping", "grace_period", duration)
	done := make(chan struct{})
	go func() {
		s.Raw.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		s.log.Info("stopped gracefully")
	case <-time.After(duration):
		s.log.Info("grace period exhausted")
		s.Raw.Stop()
	}
	return nil
}

// Listen binds the server's address. Serve does so itself when not already bound; calling Listen
// first lets a binary hold the address before dependents start dialing it: connections queue until
// Serve accepts them.
func (s *Server) Listen(ctx context.Context) error {
	_, err := s.lifecycle.Listen(s.listen)
	return err
}

func (s *Server) listen() (net.Listener, error) {
	if s.opts.useSocket() {
		// Clean up a stale socket file and make sure its directory exists.
		if err := os.RemoveAll(s.opts.SocketPath); err != nil {
			return nil, fmt.Errorf("removing existing socket [%s]: %w", s.opts.SocketPath, err)
		}
		if err := os.MkdirAll(filepath.Dir(s.opts.SocketPath), 0755); err != nil {
			return nil, fmt.Errorf("creating socket directory [%s]: %w", s.opts.SocketPath, err)
		}
		listener, err := net.Listen("unix", s.opts.SocketPath)
		if err != nil {
			return nil, fmt.Errorf("listening on socket [%s]: %w", s.opts.SocketPath, err)
		}
		if err := os.Chmod(s.opts.SocketPath, 0666); err != nil {
			return nil, fmt.Errorf("setting socket os permissions [%s]: %w", s.opts.SocketPath, err)
		}
		return listener, nil
	}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(s.opts.Port))
	if err != nil {
		return nil, fmt.Errorf("listening on port [%d]: %w", s.opts.Port, err)
	}
	return listener, nil
}

// Serve blocks until the server is stopped.
func (s *Server) Serve(ctx context.Context) error {
	listener, err := s.lifecycle.Listen(s.listen)
	if err != nil {
		return err
	}
	if s.opts.useSocket() {
		defer os.Remove(s.opts.SocketPath)
	}
	s.healthServer.Start(ctx)
	s.log.InfoContext(ctx, "serving")
	// Stopped before serving: a clean exit, like a stop during Serve.
	if err := s.Raw.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("server exited unexpectedly: %w", err)
	}
	return nil
}
