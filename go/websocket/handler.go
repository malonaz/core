package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	defaultUpgrader = &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins by default
		},
	}
)

const (
	defaultReadBufferSize  = 1000
	defaultWriteBufferSize = 1000
)

// Config holds the configuration options for a Handler
type Config struct {
	log          *slog.Logger
	onCloseFN    func(error)
	upgrader     *websocket.Upgrader
	readBufSize  int
	writeBufSize int
}

// Handler handles a single upgraded websocket connection
type Handler[Req, Resp any] struct {
	Config
	w          http.ResponseWriter
	r          *http.Request
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

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) {
		c.log = logger
	}
}

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

// NewHandler upgrades an HTTP connection to websocket and returns a Handler
func NewHandler[Req, Resp any](
	w http.ResponseWriter,
	r *http.Request,
	options ...Option,
) *Handler[Req, Resp] {
	config := Config{
		log:          slog.Default(),
		upgrader:     defaultUpgrader,
		readBufSize:  defaultReadBufferSize,
		writeBufSize: defaultWriteBufferSize,
	}

	// Apply options
	for _, opt := range options {
		opt(&config)
	}

	return &Handler[Req, Resp]{
		r:      r,
		w:      w,
		Config: config,
	}
}

// Start begins processing reads/writes from/to the websocket connection
func (h *Handler[Req, Resp]) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	// Upgrade the connection
	conn, err := h.upgrader.Upgrade(h.w, h.r, nil)
	if err != nil {
		return fmt.Errorf("upgrading connection: %w", err)
	}

	h.conn = conn
	h.readChan = make(chan Req, h.readBufSize)
	h.writeChan = make(chan Resp, h.writeBufSize)
	h.errChan = make(chan error, 1) // Buffered to prevent blocking
	h.close = make(chan struct{})

	h.log = h.log.WithGroup("websocket_handler").With("remote_addr", h.r.RemoteAddr, "path", h.r.URL.Path)
	h.log.InfoContext(ctx, "started")
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
	if err != nil {
		h.log.Error("closing", "error", err)
	} else {
		h.log.Info("closing")
	}

	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()

	select {
	case <-h.close:
		// Already closed
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

// ReadMessage blocks until a message is received, the context is cancelled, or an error occurs
func (h *Handler[Req, Resp]) ReadMessage(ctx context.Context) (Req, error) {
	var zero Req

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-h.close:
		if err := h.CloseError(); err != nil {
			return zero, fmt.Errorf("handler closed: %w", err)
		}
		return zero, io.EOF
	case msg, ok := <-h.readChan:
		if !ok {
			if err := h.CloseError(); err != nil {
				return zero, fmt.Errorf("read channel closed: %w", err)
			}
			return zero, io.EOF
		}
		return msg, nil
	}
}

// CloseError returns the error that caused the handler to close, if any
func (h *Handler[Req, Resp]) CloseError() error {
	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()
	return h.closeErr
}

func (h *Handler[Req, Resp]) read(ctx context.Context) {
	h.log.InfoContext(ctx, "starting read routine")
	defer h.log.InfoContext(ctx, "exiting read routine")
	defer close(h.readChan)

	for {
		_, bytes, err := h.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.log.ErrorContext(ctx, "reading message", "error", err)
				h.closeWithError(fmt.Errorf("read error: %w", err))
			} else {
				h.Close()
			}
			return
		}

		var payload Req
		if err := json.Unmarshal(bytes, &payload); err != nil {
			h.log.ErrorContext(ctx, "unmarshaling message", "error", err)
			h.closeWithError(fmt.Errorf("unmarshal error: %w", err))
			return
		}

		select {
		case h.readChan <- payload:
		case <-ctx.Done():
			return
		case <-h.close:
			return
		default:
			h.log.WarnContext(ctx, "read channel is full - blocking")
			select {
			case h.readChan <- payload: // Wait for buffer to free up
			case <-ctx.Done():
				return
			case <-h.close:
				return
			}
		}
	}
}

func (h *Handler[Req, Resp]) write(ctx context.Context) {
	h.log.InfoContext(ctx, "starting write routine")
	defer h.log.InfoContext(ctx, "exiting write routine")
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.close:
			return
		case payload := <-h.writeChan:
			if err := h.conn.WriteJSON(payload); err != nil {
				h.log.ErrorContext(ctx, "writing to websocket", "error", err)
				h.closeWithError(fmt.Errorf("write error: %w", err))
				return
			}
		}
	}
}
