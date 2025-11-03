package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/malonaz/core/go/health"
	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

// Opts holds HTTP server options.
type Opts struct {
	Health              *health.GRPCOpts `group:"Health" namespace:"health" env-namespace:"HEALTH"`
	Port                int              `long:"port" description:"Port to serve HTTP on" default:"8080"`
	ReadTimeout         time.Duration    `long:"read-timeout" description:"HTTP read timeout" default:"30s"`
	WriteTimeout        time.Duration    `long:"write-timeout" description:"HTTP write timeout" default:"30s"`
	IdleTimeout         time.Duration    `long:"idle-timeout" description:"HTTP idle timeout" default:"120s"`
	GracefulStopTimeout int              `long:"graceful-stop-timeout" env:"GRACEFUL_STOP_TIMEOUT" description:"How many seconds to wait for graceful stop." default:"30"`
}

// Server holds the HTTP server state.
type Server struct {
	opts         *Opts
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
		mux:          http.NewServeMux(),
		healthServer: health.NewGRPCServer(opts.Health),
		register:     register,
		patternSet:   map[string]struct{}{},
	}
}

func (s *Server) RegisterRoute(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	if _, ok := s.patternSet[pattern]; ok {
		log.Panicf("duplicate pattern registered [%s]", pattern)
	}
	s.patternSet[pattern] = struct{}{}
	s.mux.HandleFunc(pattern, handler)
}

func (s *Server) GetHealthServer() *health.GRPCServer {
	return s.healthServer
}

// Serve the HTTP server.
func (s *Server) Serve(ctx context.Context) {
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
	log.Infof("Starting HTTP server on port %d", s.opts.Port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Panicf("http server exited with error: %v", err)
	}
}

func (s *Server) Stop() {
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) GracefulStop() {
	return
	// TODO(malon): this is trickier as 'ListenAndServe' returns immediately once this is called.
	duration := time.Duration(s.opts.GracefulStopTimeout) * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	log.Infof("attempting to gracefully stop server, with a grace period of %s", duration)
	// Shutdown HTTP server
	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		//return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	log.Info("HTTP server shutdown complete")
}
