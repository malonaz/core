package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/malonaz/core/go/logging"
)

var (
	defaultUpgrader = &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins by default
		},
	}

	handlerCounter atomic.Uint64
)

const (
	defaultReadBufferSize  = 1000
	defaultWriteBufferSize = 1000
)

// Config holds the configuration options for a Handler
type Config struct {
	onCloseFN    func(error)
	upgrader     *websocket.Upgrader
	readBufSize  int
	writeBufSize int
	logger       *logging.Logger
}

// Handler handles a single upgraded websocket connection
type Handler[Req, Resp any] struct {
	Config
	conn       *websocket.Conn
	readChan   chan Req
	writeChan  chan Resp
	errChan    chan error
	close      chan struct{}
	closeMutex sync.Mutex
	closeErr   error // Store the error that caused closure
	cancel     func()
}

// Option configures a Handler
type Option func(*Config)

// WithOnClose sets the callback function called when the handler closes
// The callback receives any error that caused the closure
func WithOnClose(fn func(error)) Option {
	return func(c *Config) {
		c.onCloseFN = fn
	}
}

// WithReadBufferSize sets the size of the read channel buffer
func WithReadBufferSize(size int) Option {
	return func(c *Config) {
		c.readBufSize = size
	}
}

// WithWriteBufferSize sets the size of the write channel buffer
func WithWriteBufferSize(size int) Option {
	return func(c *Config) {
		c.writeBufSize = size
	}
}

// WithUpgrader sets a custom websocket upgrader
func WithUpgrader(upgrader *websocket.Upgrader) Option {
	return func(c *Config) {
		c.upgrader = upgrader
	}
}

// WithCheckOrigin sets a custom origin checker for the upgrader
func WithCheckOrigin(fn func(*http.Request) bool) Option {
	return func(c *Config) {
		if c.upgrader == nil {
			// Create a copy of default upgrader
			upgraderCopy := *defaultUpgrader
			c.upgrader = &upgraderCopy
		}
		c.upgrader.CheckOrigin = fn
	}
}

// WithLogger sets a custom logger for the handler
func WithLogger(logger *logging.Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}

// NewHandler upgrades an HTTP connection to websocket and returns a Handler
func NewHandler[Req, Resp any](
	w http.ResponseWriter,
	r *http.Request,
	options ...Option,
) (*Handler[Req, Resp], error) {
	config := Config{
		upgrader:     defaultUpgrader,
		readBufSize:  defaultReadBufferSize,
		writeBufSize: defaultWriteBufferSize,
	}

	// Apply options
	for _, opt := range options {
		opt(&config)
	}

	if config.logger == nil {
		config.logger = logging.NewLogger()
	}

	handler := &Handler[Req, Resp]{
		Config: config,
	}

	// Upgrade the connection
	conn, err := handler.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, fmt.Errorf("upgrading connection: %w", err)
	}

	handler.conn = conn
	handler.readChan = make(chan Req, handler.readBufSize)
	handler.writeChan = make(chan Resp, handler.writeBufSize)
	handler.errChan = make(chan error, 1) // Buffered to prevent blocking
	handler.close = make(chan struct{})

	return handler, nil
}

// Start begins processing reads/writes from/to the websocket connection
func (h *Handler[Req, Resp]) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	go h.read(ctx)
	go h.write(ctx)
	return nil
}

// Close gracefully closes this websocket connection
func (h *Handler[Req, Resp]) Close() {
	h.closeWithError(nil)
}

// closeWithError closes the handler and optionally sends an error to the error channel
func (h *Handler[Req, Resp]) closeWithError(err error) {
	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()

	select {
	case <-h.close:
		// Already closed
		return
	default:
		// Store the error
		h.closeErr = err

		if h.cancel != nil {
			h.cancel()
		}
		h.conn.Close()
		close(h.close)

		// Send error if provided (non-blocking)
		if err != nil {
			select {
			case h.errChan <- err:
			default:
				// We only send the first error up to the caller.
			}
		}
		close(h.errChan)

		if h.onCloseFN != nil {
			h.onCloseFN(err) // Pass error to callback
		}

		h.logger.Infof("Websocket handler closed")
	}
}

// Read returns the read channel
func (h *Handler[Req, Resp]) Read() <-chan Req {
	return h.readChan
}

// Write returns the write channel
func (h *Handler[Req, Resp]) Write() chan<- Resp {
	return h.writeChan
}

// Error returns the error channel
func (h *Handler[Req, Resp]) Error() <-chan error {
	return h.errChan
}

// CloseError returns the error that caused the handler to close, if any
func (h *Handler[Req, Resp]) CloseError() error {
	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()
	return h.closeErr
}

func (h *Handler[Req, Resp]) read(ctx context.Context) {
	defer close(h.readChan)

	for {
		select {
		case <-ctx.Done():
			h.logger.Infof("exiting read routine for websocket")
			return
		case <-h.close:
			return
		default:
		}

		_, bytes, err := h.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.Errorf("reading message: %v", err)
				h.closeWithError(fmt.Errorf("read error: %w", err))
			} else {
				h.Close()
			}
			return
		}

		var payload Req
		if err := json.Unmarshal(bytes, &payload); err != nil {
			h.logger.Errorf("unmarshaling message: %v", err)
			h.closeWithError(fmt.Errorf("unmarshal error: %w", err))
			return
		}

		select {
		case h.readChan <- payload:
		case <-ctx.Done():
			h.logger.Infof("exiting read routine")
			return
		case <-h.close:
			return
		default:
			h.logger.Warningf("read channel is full - blocking")
			select {
			case h.readChan <- payload: // Wait for buffer to free up
			case <-ctx.Done():
				h.logger.Infof("exiting read routine")
				return
			case <-h.close:
				return
			}
		}
	}
}

func (h *Handler[Req, Resp]) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.logger.Infof("exiting write routine for websocket")
			return
		case <-h.close:
			return
		case payload := <-h.writeChan:
			if err := h.conn.WriteJSON(payload); err != nil {
				h.logger.Errorf("writing to websocket: %v", err)
				h.closeWithError(fmt.Errorf("write error: %w", err))
				return
			}
		}
	}
}
