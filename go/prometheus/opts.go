package prometheus

import (
	"fmt"
	"net/http"

	"github.com/malonaz/core/go/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var logger = logging.NewLogger()

// Opts holds prometheus opts.
type Opts struct {
	Disable bool `long:"disable" env:"DISABLE" description:"Set to true to disable prometheus metrics"`
	Port    int  `long:"port" env:"PORT" description:"Port to serve Prometheus metrics on" default:"13434"`
}

// Serve serves prometheus in a goroutine.
func Serve(opts *Opts) {
	if opts.Disable {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	logger.Infof("Serving Prometheus metrics on [:%d/metrics]", opts.Port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), mux); err != nil {
			logger.Warningf("Prometheus server shutdown unexpectedly : %v", err)
		}
	}()

}
