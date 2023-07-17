package health

import (
	"context"
	"fmt"
	"net/http"

	"golang.org/x/sync/errgroup"

	"go/logging"
)

var log = logging.NewLogger()

// Opts holds health opts.
type Opts struct {
	Disable bool `long:"disable" description:"Set to true to disable health check"`
	Port    int  `long:"port" description:"Port to serve Health on" default:"4040"`
}

// Check defines the health check function type.
type Check func(context.Context) error

// Serve serves health in a goroutine.
func Serve(opts Opts, handlerFN Check) {
	if opts.Disable {
		return
	}
	httpHandler := func(w http.ResponseWriter, r *http.Request) {
		if err := handlerFN(r.Context()); err != nil {
			log.Errorf("Health check failed: (%v)", err)
			http.Error(w, "Health check failed", http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", httpHandler)
	log.Infof("Serving health check on [:%d/healthz]", opts.Port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), mux); err != nil {
			log.Warningf("Health server shutdown unexpectedly : %v", err)
		}
	}()

}

// Checks combines several checks into a single one. It runs each health check in parallel.
func Checks(checks ...Check) Check {
	return func(ctx context.Context) error {
		errGroup, ctx := errgroup.WithContext(ctx)
		for _, check := range checks {
			check := check
			fn := func() error { return check(ctx) }
			errGroup.Go(fn)
		}
		return errGroup.Wait()
	}
}
