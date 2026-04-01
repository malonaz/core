package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

const (
	defaultReadBufferSize  = 1000
	defaultWriteBufferSize = 1000
)

// Config holds the configuration options for a Handler.
type Config struct {
	log           *slog.Logger
	onCloseFN     func(error)
	acceptOptions *websocket.AcceptOptions
	readBufSize   int
	writeBufSize  int
}

// Handler handles a single upgraded websocket connection. It owns the read/write
// goroutines and exposes typed channels so callers never touch the raw connection.
type Handler[Req, Resp any] struct {
	Config
	w                  http.ResponseWriter
	r                  *http.Request
	conn               *websocket.Conn
	readChan           chan Req
	writeChan          chan Resp
	errChan            chan error
	close              chan struct{}
	writeChanClosed    sync.Once
	writeRoutineExited chan struct{}
	closeMutex         sync.Mutex
	closeErr           error
	cancel             func()
}

// Option configures a Handler.
type Option func(*Config)

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) {
		c.log = logger
	}
}

// WithOnClose sets the callback function called when the handler closes.
// The callback receives any error that caused the closure, or nil for a clean close.
func WithOnClose(fn func(error)) Option {
	return func(c *Config) {
		c.onCloseFN = fn
	}
}

// WithReadBufferSize sets the size of the read channel buffer.
func WithReadBufferSize(size int) Option {
	return func(c *Config) {
		c.readBufSize = size
	}
}

// WithWriteBufferSize sets the size of the write channel buffer.
func WithWriteBufferSize(size int) Option {
	return func(c *Config) {
		c.writeBufSize = size
	}
}

// WithAcceptOptions sets the full AcceptOptions for the underlying coder/websocket.Accept call.
func WithAcceptOptions(opts *websocket.AcceptOptions) Option {
	return func(c *Config) {
		c.acceptOptions = opts
	}
}

// WithInsecureSkipVerify controls whether origin checking is skipped during the
// websocket upgrade. Defaults to true (allow all origins) to match common dev usage.
func WithInsecureSkipVerify(skip bool) Option {
	return func(c *Config) {
		if c.acceptOptions == nil {
			c.acceptOptions = &websocket.AcceptOptions{}
		}
		c.acceptOptions.InsecureSkipVerify = skip
	}
}

// WithOriginPatterns sets allowed origin patterns for the websocket upgrade.
// Only relevant when InsecureSkipVerify is false.
func WithOriginPatterns(patterns ...string) Option {
	return func(c *Config) {
		if c.acceptOptions == nil {
			c.acceptOptions = &websocket.AcceptOptions{}
		}
		c.acceptOptions.OriginPatterns = patterns
	}
}

// NewHandler creates a Handler for an incoming HTTP request. Call Start to upgrade
// the connection and begin processing.
func NewHandler[Req, Resp any](
	w http.ResponseWriter,
	r *http.Request,
	options ...Option,
) *Handler[Req, Resp] {
	config := Config{
		log: slog.Default(),
		// Default to allowing all origins, matching the original gorilla upgrader behaviour.
		acceptOptions: &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		},
		readBufSize:  defaultReadBufferSize,
		writeBufSize: defaultWriteBufferSize,
	}

	for _, opt := range options {
		opt(&config)
	}

	return &Handler[Req, Resp]{
		r:      r,
		w:      w,
		Config: config,
	}
}

// Start upgrades the HTTP connection to a websocket and spawns background read/write goroutines.
func (h *Handler[Req, Resp]) Start(ctx context.Context) error {
	// Derive a cancellable context so Close() can tear down both goroutines.
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	conn, err := websocket.Accept(h.w, h.r, h.acceptOptions)
	if err != nil {
		return fmt.Errorf("upgrading connection: %w", err)
	}

	h.conn = conn
	h.readChan = make(chan Req, h.readBufSize)
	h.writeChan = make(chan Resp, h.writeBufSize)
	// Buffered to 1 so closeWithError never blocks when sending the first error.
	h.errChan = make(chan error, 1)
	h.close = make(chan struct{})
	h.writeRoutineExited = make(chan struct{})

	h.log = h.log.WithGroup("websocket_handler").With("remote_addr", h.r.RemoteAddr, "path", h.r.URL.Path)
	go h.read(ctx)
	go h.write(ctx)
	return nil
}

// DrainWrites closes the write channel and waits for all buffered writes to complete.
// This allows a graceful shutdown where pending messages are flushed before the
// connection is torn down.
func (h *Handler[Req, Resp]) DrainWrites(ctx context.Context) {
	// sync.Once ensures the channel is only closed once even if DrainWrites is
	// called concurrently.
	h.writeChanClosed.Do(func() {
		close(h.writeChan)
	})

	// Wait for the write goroutine to finish draining.
	select {
	case <-h.writeRoutineExited:
	case <-ctx.Done():
	case <-h.close:
	}
}

// Close gracefully closes this websocket connection.
func (h *Handler[Req, Resp]) Close() {
	h.closeWithError(nil)
}

// closeWithError tears down the handler exactly once. The mutex + closed-channel
// check guarantees idempotency so concurrent callers (read/write goroutines, user
// code) are safe.
func (h *Handler[Req, Resp]) closeWithError(err error) {
	if err != nil {
		h.log.Error("closing", "error", err)
	}

	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()

	select {
	case <-h.close:
		// Already closed — nothing to do.
	default:
		h.closeErr = err

		// Cancel the context to unblock any pending conn.Read/conn.Write calls.
		if h.cancel != nil {
			h.cancel()
		}

		// Attempt a graceful close by sending a StatusNormalClosure close frame to
		// the peer. If the graceful handshake fails (e.g. broken pipe, peer already
		// gone), fall back to an immediate close to ensure the underlying connection
		// is always released.
		if closeErr := h.conn.Close(websocket.StatusNormalClosure, ""); closeErr != nil {
			h.conn.CloseNow()
		}

		close(h.close)

		// Non-blocking send: we only propagate the first error to the caller.
		if err != nil {
			select {
			case h.errChan <- err:
			default:
			}
		}
		close(h.errChan)

		// Notify the owner after all teardown is complete so the callback can
		// safely inspect handler state (e.g. CloseError).
		if h.onCloseFN != nil {
			h.onCloseFN(err)
		}
	}
}

// Read returns the channel that receives deserialized client messages.
func (h *Handler[Req, Resp]) Read() <-chan Req {
	return h.readChan
}

// Write returns the channel used to send messages to the client.
func (h *Handler[Req, Resp]) Write() chan<- Resp {
	return h.writeChan
}

// Error returns a channel that receives the first fatal error, if any.
func (h *Handler[Req, Resp]) Error() <-chan error {
	return h.errChan
}

// ReadMessage blocks until a message is received, the context is cancelled, or an error occurs.
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

// CloseError returns the error that caused the handler to close, if any.
func (h *Handler[Req, Resp]) CloseError() error {
	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()
	return h.closeErr
}

// read continuously reads from the websocket, deserializes JSON messages, and pushes
// them into readChan. On error it determines whether the close was expected (normal
// shutdown, going away, abnormal) or truly unexpected, and shuts down accordingly.
func (h *Handler[Req, Resp]) read(ctx context.Context) {
	h.log.DebugContext(ctx, "starting read routine")
	defer h.log.DebugContext(ctx, "exiting read routine")
	// Closing readChan signals consumers that no more messages will arrive.
	defer close(h.readChan)

	for {
		// coder/websocket's Read is context-aware, so it will unblock when ctx is
		// cancelled (e.g. via Close → cancel).
		_, bytes, err := h.conn.Read(ctx)
		if err != nil {
			// Determine whether this is a recoverable/expected close or a fatal error.
			// This mirrors gorilla's IsUnexpectedCloseError logic:
			//   - closeStatus == -1: not a websocket close frame at all (network error,
			//     context cancellation, etc.) → treat as normal teardown.
			//   - Expected close codes (normal, going away, abnormal) → clean close.
			//   - Any other close code → unexpected, propagate as error.
			closeStatus := websocket.CloseStatus(err)
			isExpected := closeStatus == -1 ||
				closeStatus == websocket.StatusNormalClosure ||
				closeStatus == websocket.StatusGoingAway ||
				closeStatus == websocket.StatusAbnormalClosure
			if !isExpected {
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

		// Try a non-blocking send first; if the channel is full, log a warning and
		// then block. This provides backpressure visibility without dropping messages.
		select {
		case h.readChan <- payload:
		case <-ctx.Done():
			return
		case <-h.close:
			return
		default:
			h.log.WarnContext(ctx, "read channel is full - blocking")
			select {
			case h.readChan <- payload:
			case <-ctx.Done():
				return
			case <-h.close:
				return
			}
		}
	}
}

// write drains writeChan and sends each message as JSON over the websocket. It exits
// when the channel is closed (via DrainWrites), the context is cancelled, or an error
// occurs.
func (h *Handler[Req, Resp]) write(ctx context.Context) {
	h.log.DebugContext(ctx, "starting write routine")
	defer h.log.DebugContext(ctx, "exiting write routine")
	// Signal that the write goroutine has exited so DrainWrites can return.
	defer close(h.writeRoutineExited)

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.close:
			return
		case payload, ok := <-h.writeChan:
			if !ok {
				// writeChan was closed by DrainWrites — all buffered messages have
				// been consumed via the channel range, so we're done.
				return
			}
			// Marshal manually since coder/websocket has no WriteJSON convenience method.
			bytes, err := json.Marshal(payload)
			if err != nil {
				h.log.ErrorContext(ctx, "marshaling message", "error", err)
				h.closeWithError(fmt.Errorf("marshal error: %w", err))
				return
			}
			// coder/websocket's Write is context-aware and will unblock on cancellation.
			if err := h.conn.Write(ctx, websocket.MessageText, bytes); err != nil {
				h.log.ErrorContext(ctx, "writing to websocket", "error", err)
				h.closeWithError(fmt.Errorf("write error: %w", err))
				return
			}
		}
	}
}
