package prometheus

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Opts holds prometheus opts.
type Opts struct {
	Disable bool `long:"disable" env:"DISABLE" description:"Set to true to disable prometheus metrics"`
	Port    int  `long:"port" env:"PORT" description:"Port to serve Prometheus metrics on" default:"13434"`
}

func (o *Opts) Enabled() bool {
	return o != nil && !o.Disable
}

type Server struct {
	opts   *Opts
	log    *slog.Logger
	server *http.Server
}

func NewServer(opts *Opts) *Server {
	return &Server{
		opts: opts,
		log:  slog.Default(),
	}
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger
	return s
}

func (s *Server) Start(ctx context.Context) {
	if !s.opts.Enabled() {
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.opts.Port),
		Handler: mux,
	}

	s.log.Info("serving Prometheus metrics", "port", s.opts.Port, "endpoint", "/metrics")
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.log.Warn("prometheus server shutdown unexpectedly", "error", err)
	}
}

func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.log.Info("stopping Prometheus server")
	if err := s.server.Shutdown(ctx); err != nil {
		s.log.Error("Prometheus server forced to shutdown", "error", err)
		return err
	}
	s.log.Info("Prometheus server stopped gracefully")
	return nil
}
