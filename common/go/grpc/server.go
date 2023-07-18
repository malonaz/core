package grpc

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/bufbuild/protovalidate-go"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"common/go/certs"
	"common/go/health"
	"common/go/prometheus"
)

const (
	// maximum message size for the server (20 MB)
	maximumMessageSize         = 20 * 1024 * 1024
	gracefulStopTimeoutSeconds = 10
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
	opts           Opts
	prometheusOpts prometheus.Opts
	register       func(*Server)
	Raw            *grpc.Server

	healthCheck health.Check
	// The first interceptor is called first.
	unaryInterceptors []grpc.UnaryServerInterceptor
	// The first interceptor is called first.
	streamInterceptors []grpc.StreamServerInterceptor
	options            []grpc.ServerOption
}

// NewServer creates and returns a new Server.
func NewServer(opts Opts, certsOpts certs.Opts, prometheusOpts prometheus.Opts, register func(*Server)) *Server {
	server := &Server{
		opts:           opts,
		prometheusOpts: prometheusOpts,
		register:       register,
	}

	// Default options.
	server.options = append(server.options, grpc.MaxRecvMsgSize(maximumMessageSize), grpc.MaxSendMsgSize(maximumMessageSize))
	if !opts.DisableTLS {
		tlsConfig, err := certsOpts.ServerTLSConfig()
		if err != nil {
			log.Panicf("Could not load server TLS config: %v", err)
		}
		server.options = append(server.options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Warningf("Starting gRPC server without TLS")
	}

	// Default interceptors.
	if !prometheusOpts.Disable {
		server.unaryInterceptors = append(server.unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
		server.streamInterceptors = append(server.streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	}
	// Always pass logging first, so that subsequent interceptors have error logging enabled :).
	server.unaryInterceptors = append(server.unaryInterceptors, unaryServerLoggingInterceptor(), unaryServerContextPropagationInterceptor(), unaryServerValidateInterceptor())
	server.streamInterceptors = append(server.streamInterceptors, streamServerLoggingInterceptor(), streamServerContextPropagationInterceptor(), streamServerValidateInterceptor())
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

// WithUnaryInterceptors adds interceptors to this gRPC server.
func (s *Server) WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) *Server {
	s.unaryInterceptors = append(s.unaryInterceptors, interceptors...)
	return s
}

// WithStreamInterceptors adds interceptors to this gRPC server.
func (s *Server) WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) *Server {
	s.streamInterceptors = append(s.streamInterceptors, interceptors...)
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
	if len(s.unaryInterceptors) > 0 {
		s.options = append(s.options, grpc.ChainUnaryInterceptor(s.unaryInterceptors...))
	}
	if len(s.streamInterceptors) > 0 {
		s.options = append(s.options, grpc.ChainStreamInterceptor(s.streamInterceptors...))
	}

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
		grpc_health_v1.RegisterHealthServer(s.Raw, s)
	}
	go handleSignals(func() { s.gracefulStop(s.Raw) }, s.Raw.Stop)
	if !s.prometheusOpts.Disable {
		grpc_prometheus.Register(s.Raw)
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	if err := s.Raw.Serve(listener); err != nil {
		log.Panicf("gRPC server exited with error: %v", err)
	}
}

// Check implements the grpc health v1 interface.
func (s *Server) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	status := grpc_health_v1.HealthCheckResponse_SERVING
	if err := s.healthCheck(ctx); err != nil {
		status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}
	return &grpc_health_v1.HealthCheckResponse{Status: status}, nil
}

// Watch implements the grpc health v1 interface.
func (s *Server) Watch(in *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

// unaryServerContextPropagationInterceptor propagates incoming context to downstream calls.
func unaryServerContextPropagationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		return handler(ctx, req)
	}
}

// streamServerContextPropagationInterceptor propagates incoming context to downstream calls.
func streamServerContextPropagationInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(srv, stream)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

func unaryServerLoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		if err != nil {
			log.Errorf(err.Error())
		}
		return response, err
	}
}

func streamServerLoggingInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := handler(srv, stream); err != nil {
			log.Error(err.Error())
		}
		return nil
	}
}

// unaryServerValidateInterceptor returns a new unary server interceptor that validates incoming messages.
// Invalid messages will be rejected with `InvalidArgument` before reaching any userspace handlers.
func unaryServerValidateInterceptor() grpc.UnaryServerInterceptor {
	validator, err := protovalidate.New()
	if err != nil {
		log.Panicf("could not instantiate proto validator")
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validator.Validate(req.(proto.Message)); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return handler(ctx, req)
	}
}

// streamServerValidateInterceptor returns a new streaming server interceptor that validates incoming messages.
// The stage at which invalid messages will be rejected with `InvalidArgument` varies based on the
// type of the RPC. For `ServerStream` (1:m) requests, it will happen before reaching any userspace
// handlers. For `ClientStream` (n:1) or `BidiStream` (n:m) RPCs, the messages will be rejected on
// calls to `stream.Recv()`.
func streamServerValidateInterceptor() grpc.StreamServerInterceptor {
	validator, err := protovalidate.New()
	if err != nil {
		log.Panicf("could not instantiate proto validator")
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapper := &recvWrapperValidate{
			ServerStream: stream,
			validator: validator,
		}
		return handler(srv, wrapper)
	}
}

type recvWrapperValidate struct {
	validator *protovalidate.Validator
	grpc.ServerStream
}

func (s *recvWrapperValidate) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if err := s.validator.Validate(m.(proto.Message)); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
