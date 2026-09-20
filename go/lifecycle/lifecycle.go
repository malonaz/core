// Package lifecycle sequences a server's Listen, Stop and GracefulStop across goroutines. The raw
// servers refuse to serve once stopped, so what is left to sequence is ours: a listener bound before
// Serve adopted it, and stops that repeat.
package lifecycle

import (
	"net"
	"sync"
)

type phase int

const (
	serving phase = iota
	draining
	stopped
)

// State is the zero-value-ready lifecycle of one server.
type State struct {
	mu       sync.Mutex
	listener net.Listener
	phase    phase
}

// Listen binds through bind once and returns the listener; later calls return the same listener.
func (s *State) Listen(bind func() (net.Listener, error)) (net.Listener, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener, nil
	}
	listener, err := bind()
	if err != nil {
		return nil, err
	}
	s.listener = listener
	return listener, nil
}

// GracefulStop records a graceful stop and reports whether the caller should perform it: false once
// any stop has been requested.
func (s *State) GracefulStop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != serving {
		return false
	}
	s.phase = draining
	return true
}

// Stop records a forced stop and reports whether the caller should perform it: false once a stop is
// complete. It does follow a GracefulStop, forcing a drain that has not finished.
func (s *State) Stop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase == stopped {
		return false
	}
	s.phase = stopped
	return true
}

// Done marks the stop complete, so a following Stop has nothing left to force, and releases the
// listener in case Serve never adopted it: a server closes the listener it serves, and closing one
// twice is harmless.
func (s *State) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = stopped
	if s.listener != nil {
		s.listener.Close()
	}
}
