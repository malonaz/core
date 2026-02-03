package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/malonaz/core/go/health"
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
	register     func(*Server)
	patternSet   map[string]struct{}
}

// NewServer creates a new HTTP server.
func NewServer(opts *Opts, register func(*Server)) *Server {
	return &Server{
		opts:         opts,
		log:          slog.Default(),
		mux:          http.NewServeMux(),
		healthServer: health.NewGRPCServer(opts.Health),
		register:     register,
		patternSet:   map[string]struct{}{},
	}
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

// Serve the HTTP server.
func (s *Server) Serve(ctx context.Context) error {
	s.register(s)
	// Start health server in background
	go s.healthServer.Start(ctx)

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.opts.Port),
		Handler:      s.mux,
		ReadTimeout:  s.opts.ReadTimeout,
		WriteTimeout: s.opts.WriteTimeout,
		IdleTimeout:  s.opts.IdleTimeout,
	}

	// Start HTTP server
	s.log.InfoContext(ctx, "starting HTTP server", "port", s.opts.Port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server exited unexpectedly: %w", err)
	}
	return nil
}

// Stop immediately stops the gateway server.
func (s *Server) Stop() error {
	if s.httpServer == nil {
		return nil
	}
	s.log.Info("stopping HTTP server")
	s.healthServer.Shutdown()
	return s.httpServer.Close()
}

// GracefulStop gracefully stops the gateway server.
func (s *Server) GracefulStop() error {
	if s.httpServer == nil {
		return nil
	}
	s.log.Info("gracefully stopping gRPC Gateway")
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	if err == context.DeadlineExceeded {
		s.log.Warn("graceful shutdown timed out")
		// Force close any remaining connections
		return s.Stop()
	}
	return err
}
