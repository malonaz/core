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

	var err error
	var errMutex = &sync.RWMutex{}
	go assertHealthPeriodically(handlerFN, &err, errMutex)
	mux := http.NewServeMux()

	mux.HandleFunc("/readiness", func(w http.ResponseWriter, r *http.Request) {
		errMutex.RLock()
		defer errMutex.RUnlock()

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

	log.Infof("Serving health check on [:%d]", opts.Port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), mux); err != nil {
			log.Warningf("Health server shutdown unexpectedly : %v", err)
		}
	}()
}

func assertHealthPeriodically(check Check, healthCheckErr *error, errMutex *sync.RWMutex) {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		err := check(context.Background())

		errMutex.Lock()
		*healthCheckErr = err
		errMutex.Unlock()
	}
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
