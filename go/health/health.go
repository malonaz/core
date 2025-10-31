package health

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

// Opts holds health opts.
type Opts struct {
	Disable         bool `long:"disable" description:"Set to true to disable health check"`
	Port            int  `long:"port" description:"Port to serve Health on" default:"4040"`
	IntervalSeconds int  `long:"interval-seconds" description:"Health check interval in seconds" default:"10"`
	TimeoutSeconds  int  `long:"timeout-seconds" description:"Health check timeout in seconds" default:"30"`
}

// Check defines the health check function type.
type Check func(context.Context) error

// Server holds the health check server state.
type Server struct {
	opts      *Opts
	handlerFn Check
	ready     bool
	err       error
	mutex     sync.RWMutex
	server    *http.Server
}

// NewServer creates a new health check server.
func NewServer(opts *Opts, handlerFn Check) *Server {
	return &Server{
		opts:      opts,
		handlerFn: handlerFn,
		err:       fmt.Errorf("health check has not run yet"),
	}
}

func (s *Server) isReady() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.ready
}

func (s *Server) getErr() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.err
}

// MarkReady marks the server as ready to serve traffic.
// This should be called when your application has finished initialization.
func (s *Server) MarkReady() {
	s.mutex.Lock()
	s.ready = true
	s.mutex.Unlock()
	log.Infof("Health server marked as ready")
}

// Serve starts the health check server.
func (s *Server) Serve(ctx context.Context) {
	if s.opts.Disable {
		return
	}

	// Start health check routine
	go s.assertHealthPeriodically(ctx)

	// Setup HTTP handlers
	mux := http.NewServeMux()

	mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
		if !s.isReady() {
			log.Debugf("Readiness check failed: server not ready")
			http.Error(w, "Server not ready", http.StatusServiceUnavailable)
			return
		}

		err := s.getErr()
		if err != nil {
			log.Errorf("Health check failed: (%v)", err)
			http.Error(w, "Health check failed", http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/liveness", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	// Create and start HTTP server
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.opts.Port),
		Handler: mux,
	}

	log.Infof("Serving health check on [:%d]", s.opts.Port)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warningf("Health server shutdown unexpectedly : %v", err)
	}
}

func (s *Server) assertHealthPeriodically(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.opts.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isReady() {
				continue
			}
			s.assertHealth(ctx)
		}
	}
}

func (s *Server) assertHealth(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(s.opts.TimeoutSeconds)*time.Second)
	defer cancel()
	err := s.handlerFn(checkCtx)
	s.mutex.Lock()
	s.err = err
	s.mutex.Unlock()
}

// Checks combines several checks into a single one. It runs each health check in parallel.
func Checks(checks ...Check) Check {
	return func(ctx context.Context) error {
		errGroup, ctx := errgroup.WithContext(ctx)
		for i, check := range checks {
			check := check
			iCopy := i
			fn := func() error {
				err := check(ctx)
				if err != nil {
					log.Debugf("heatlh: check %d error: %v", iCopy, err)
				}

				return err
			}
			errGroup.Go(fn)
		}
		return errGroup.Wait()
	}
}
