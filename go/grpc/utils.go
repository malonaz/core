package grpc

import (
	"os"
	"os/signal"
	"syscall"
)

// handleSignals received SIGTERM / SIGINT etc to gracefully shut down a gRPC server.
// Repeated signals cause the server to terminate at increasing levels of urgency.
func handleSignals(callbacks ...func()) {
	c := make(chan os.Signal, len(callbacks)) // Channel should be buffered a bit
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	i := 0
	for sig := range c {
		log.Infof("Received signal #%d: %v", i+1, sig)
		go callbacks[i]()
		i++
	}
}
