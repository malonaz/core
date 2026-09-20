package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/lifecycle"
)

// Opts holds HTTP server options.
type Opts struct {
	Health              *health.GRPCOpts `group:"Health" namespace:"health" env-namespace:"HEALTH"`
	Port                int              `long:"port" env:"PORT" description:"Port to serve HTTP on" default:"8080"`
	ReadTimeout         time.Duration    `long:"read-timeout" env:"READ_TIMEOUT" description:"HTTP read timeout" default:"30s"`
	WriteTimeout        time.Duration    `long:"write-timeout" env:"WRITE_TIMEOUT" description:"HTTP write timeout" default:"30s"`
	IdleTimeout         time.Duration    `long:"idle-timeout" env:"IDLE_TIMEOUT"  description:"HTTP idle timeout" default:"120s"`
	GracefulStopTimeout int              `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
}

// Server holds the HTTP server state.
type Server struct {
	opts         *Opts
	log          *slog.Logger
	httpServer   *http.Server
	mux          *http.ServeMux
	healthServer *health.GRPCServer
	patternSet   map[string]struct{}
	lifecycle    lifecycle.State
}

// NewServer creates a new HTTP server and lets register add its routes.
func NewServer(opts *Opts, name string, register func(*Server)) *Server {
	mux := http.NewServeMux()
	s := &Server{
		opts:         opts,
		log:          slog.Default(),
		mux:          mux,
		healthServer: health.NewGRPCServer(opts.Health, name),
		patternSet:   map[string]struct{}{},
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", opts.Port),
			Handler:      mux,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
			IdleTimeout:  opts.IdleTimeout,
		},
	}
	register(s)
	return s
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger
	return s
}

func (s *Server) RegisterRoute(pattern string, handler func(http.ResponseWriter, *http.Request)) error {
	if _, ok := s.patternSet[pattern]; ok {
		return fmt.Errorf("duplicate pattern registered [%s]", pattern)
	}
	s.patternSet[pattern] = struct{}{}
	s.mux.HandleFunc(pattern, handler)
	return nil
}

func (s *Server) GetHealthServer() *health.GRPCServer {
	return s.healthServer
}

// Listen binds the server's address; Serve does so itself when not already bound.
func (s *Server) Listen(ctx context.Context) error {
	_, err := s.listen()
	return err
}

func (s *Server) listen() (net.Listener, error) {
	return s.lifecycle.Listen(func() (net.Listener, error) {
		listener, err := net.Listen("tcp", s.httpServer.Addr)
		if err != nil {
			return nil, fmt.Errorf("listening on port [%d]: %w", s.opts.Port, err)
		}
		return listener, nil
	})
}

// Serve the HTTP server.
func (s *Server) Serve(ctx context.Context) error {
	// Start health server in background
	go s.healthServer.Start(ctx)

	listener, err := s.listen()
	if err != nil {
		return err
	}

	s.log.InfoContext(ctx, "starting HTTP server",
		"port", s.opts.Port,
		"read_timeout", s.opts.ReadTimeout,
		"write_timeout", s.opts.WriteTimeout,
		"idle_timeout", s.opts.IdleTimeout,
		"graceful_stop_timeout", s.opts.GracefulStopTimeout,
	)
	// Stopped before serving: a clean exit, like a stop during Serve.
	if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server exited unexpectedly: %w", err)
	}
	return nil
}

// Stop closes the HTTP server immediately. A no-op once stopped.
func (s *Server) Stop() error {
	if !s.lifecycle.Stop() {
		return nil
	}
	defer s.lifecycle.Done()
	s.log.Info("stopping HTTP server")
	s.healthServer.Shutdown()
	return s.httpServer.Close()
}

// GracefulStop stops the HTTP server, waiting for in-flight requests up to the graceful stop timeout.
// A no-op once a stop has been requested.
func (s *Server) GracefulStop() error {
	if !s.lifecycle.GracefulStop() {
		return nil
	}
	defer s.lifecycle.Done()
	s.log.Info("gracefully stopping HTTP Server")
	s.healthServer.Shutdown()
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.log.Warn("graceful shutdown failed, forcing", "error", err)
		return s.httpServer.Close()
	}
	return nil
}
