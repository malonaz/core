package health

import (
	"context"
	"fmt"
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
	ready      bool
	mutex      sync.RWMutex
	httpServer *http.Server
}

// NewServer creates a new health check server.
func NewServer(opts *Opts) *Server {
	return &Server{
		GRPCServer: NewGRPCServer(opts.GRPCOpts),
		opts:       opts,
	}
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
	log.Infof("Health server marked as ready")
}

// Serve starts the HTTP health check server.
func (s *Server) Serve(ctx context.Context) {
	if s.opts.Disable {
		return
	}

	// Start the GRPC server.
	s.GRPCServer.Start(ctx)

	// Setup HTTP handlers
	mux := http.NewServeMux()

	mux.HandleFunc("/liveness", func(w http.ResponseWriter, r *http.Request) {
		if s.isReady() {
			w.Write([]byte("ok"))
		} else {
			http.Error(w, "Server not ready", http.StatusServiceUnavailable)
		}
	})

	mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
		// Check if server is ready first
		if !s.isReady() {
			log.Debugf("Readiness check failed: server not ready")
			http.Error(w, "Server not ready", http.StatusServiceUnavailable)
			return
		}

		// Use the List endpoint to get all services and their health status
		healthListRequest := &grpc_health_v1.HealthListRequest{}
		healthListResponse, err := s.List(r.Context(), healthListRequest)
		if err != nil {
			log.Debugf("Readiness check failed: %v", err)
			http.Error(w, "Failed to carry out the readiness check", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		healthCheckResponse := healthListResponse.Statuses[""]
		if healthCheckResponse.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		// Use pbutil.JSONMarshal instead of json.NewEncoder
		responseBytes, err := pbutil.JSONMarshal(healthListResponse)
		if err != nil {
			log.Errorf("Failed to marshal list response: %v", err)
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		if _, err := w.Write(responseBytes); err != nil {
			log.Errorf("Failed to write response: %v", err)
		}
	})

	// Create and start HTTP server
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.opts.Port),
		Handler: mux,
	}

	log.Infof("Serving health check on [:%d]", s.opts.Port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warningf("Health server shutdown unexpectedly: %v", err)
	}
}
