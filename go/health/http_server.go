package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Opts holds health opts.
type Opts struct {
	*GRPCOpts
	Disable bool `long:"disable" description:"Set to true to disable health check"`
	Port    int  `long:"port" description:"Port to serve Health on" default:"4040"`
}

// Server holds the health check server state and embeds gRPC health server.
type Server struct {
	*GRPCServer
	opts       *Opts
	log        *slog.Logger
	name       string
	ready      bool
	mutex      sync.RWMutex
	httpServer *http.Server
	// Fail /liveness, not /readiness: what only a restart recovers from.
	nameToLivenessCheck map[string]Check
}

// NewServer creates a new health check server.
func NewServer(opts *Opts, name string) *Server {
	s := &Server{
		GRPCServer:          NewGRPCServer(opts.GRPCOpts, name),
		opts:                opts,
		name:                name,
		nameToLivenessCheck: map[string]Check{},
	}
	s.WithLogger(slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/liveness", s.liveness)
	mux.HandleFunc("/readiness", s.readiness)
	// Built up front so a Shutdown that lands before Serve is honored: net/http refuses to serve once closed.
	s.httpServer = &http.Server{Addr: fmt.Sprintf(":%d", opts.Port), Handler: mux}
	return s
}

func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.log = logger.WithGroup("health_http_server").With("name", s.name, "port", s.opts.Port, "disable", s.opts.Disable)
	s.GRPCServer.WithLogger(logger)
	return s
}

func (s *Server) isReady() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.ready
}

// MarkReady marks the server as ready to serve traffic.
// This should be called when your application has finished initialization.
func (s *Server) MarkReady() {
	s.mutex.Lock()
	s.ready = true
	s.mutex.Unlock()
	s.log.Info("Health server marked as ready")
}

// RegisterLivenessCheck fails /liveness while check fails, so the binary is restarted. Register
// before Serve.
func (s *Server) RegisterLivenessCheck(name string, check Check) {
	s.nameToLivenessCheck[name] = check
}

// Serve starts the HTTP health check server.
func (s *Server) Serve(ctx context.Context) {
	if s.opts.Disable {
		return
	}

	// Start the GRPC server.
	s.GRPCServer.Start(ctx)

	s.log.InfoContext(ctx, "serving health check")
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.WarnContext(ctx, "health server shutdown unexpectedly", "error", err)
	}
}

// Shutdown stops the health monitoring and closes the HTTP listener.
func (s *Server) Shutdown() {
	s.GRPCServer.Shutdown()
	s.httpServer.Close()
}

func (s *Server) liveness(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		http.Error(w, "Server not ready", http.StatusServiceUnavailable)
		return
	}
	for name, check := range s.nameToLivenessCheck {
		if err := check(r.Context()); err != nil {
			s.log.WarnContext(r.Context(), "liveness check failed", "check", name, "error", err)
			http.Error(w, fmt.Sprintf("%s: %v", name, err), http.StatusServiceUnavailable)
			return
		}
	}
	w.Write([]byte("ok"))
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		s.log.DebugContext(r.Context(), "readiness check failed: server not ready")
		http.Error(w, "Server not ready", http.StatusServiceUnavailable)
		return
	}

	// Use the List endpoint to get all services and their health status
	healthListRequest := &grpc_health_v1.HealthListRequest{}
	healthListResponse, err := s.List(r.Context(), healthListRequest)
	if err != nil {
		s.log.DebugContext(r.Context(), "readiness check failed", "error", err)
		http.Error(w, "Failed to carry out the readiness check", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	healthCheckResponse := healthListResponse.Statuses[""]
	if healthCheckResponse.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	responseBytes, err := pbutil.JSONMarshal(healthListResponse)
	if err != nil {
		s.log.ErrorContext(r.Context(), "marshaling list response", "error", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(responseBytes); err != nil {
		s.log.ErrorContext(r.Context(), "writing response", "error", err)
	}
}
