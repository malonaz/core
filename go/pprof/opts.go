package pprof

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
)

// Opts holds pprof opts.
type Opts struct {
	Enable bool `long:"enable" env:"ENABLE" description:"Set to true to serve pprof profiles"`
	Port   int  `long:"port" env:"PORT" description:"Port to serve pprof profiles on" default:"6060"`
}

func (o *Opts) Enabled() bool {
	return o != nil && o.Enable
}

type Server struct {
	opts   *Opts
	log    *slog.Logger
	server *http.Server
}

func NewServer(opts *Opts) *Server {
	s := &Server{opts: opts, log: slog.Default()}
	if opts.Enabled() {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// Built up front so a Stop that lands before Start is honored: net/http refuses to serve once closed.
		s.server = &http.Server{Addr: fmt.Sprintf(":%d", opts.Port), Handler: mux}
	}
	return s
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger
	return s
}

func (s *Server) Start(ctx context.Context) {
	if !s.opts.Enabled() {
		return
	}
	s.log.Info("serving pprof", "port", s.opts.Port, "endpoint", "/debug/pprof/")
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Warn("pprof server shutdown unexpectedly", "error", err)
	}
}

func (s *Server) Stop(ctx context.Context) error {
	if !s.opts.Enabled() {
		return nil
	}
	s.log.Info("stopping pprof server")
	if err := s.server.Shutdown(ctx); err != nil {
		s.log.Error("pprof server forced to shutdown", "error", err)
		return err
	}
	s.log.Info("pprof server stopped gracefully")
	return nil
}
