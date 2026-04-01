package websocket

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/go-json-experiment/json"
)

const (
	defaultConnectRetryBackoff = 2 * time.Second
	defaultConnectTimeout      = 10 * time.Second
)

// ClientConfig holds the configuration options for a Client.
type ClientConfig struct {
	log                 *slog.Logger
	onConnectFN         func(context.Context)
	httpHeader          http.Header
	dialOptions         *websocket.DialOptions
	readBufSize         int
	writeBufSize        int
	connectRetryBackoff time.Duration
	connectTimeout      time.Duration
}

// ClientOption configures a Client.
type ClientOption func(*ClientConfig)

// WithClientLogger sets the logger.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *ClientConfig) {
		c.log = logger
	}
}

// WithOnConnect sets a callback function called when the websocket is connected (or reconnected).
func WithOnConnect(fn func(context.Context)) ClientOption {
	return func(c *ClientConfig) {
		c.onConnectFN = fn
	}
}

// WithHTTPHeader sets custom HTTP headers for the websocket upgrade request.
func WithHTTPHeader(header http.Header) ClientOption {
	return func(c *ClientConfig) {
		c.httpHeader = header
	}
}

// WithDialOptions sets custom dial options for the underlying coder/websocket.Dial call.
func WithDialOptions(opts *websocket.DialOptions) ClientOption {
	return func(c *ClientConfig) {
		c.dialOptions = opts
	}
}

// WithClientReadBufferSize sets the size of the read channel buffer.
func WithClientReadBufferSize(size int) ClientOption {
	return func(c *ClientConfig) {
		c.readBufSize = size
	}
}

// WithClientWriteBufferSize sets the size of the write channel buffer.
func WithClientWriteBufferSize(size int) ClientOption {
	return func(c *ClientConfig) {
		c.writeBufSize = size
	}
}

// WithConnectRetryBackoff sets the backoff duration between connection retry attempts.
func WithConnectRetryBackoff(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.connectRetryBackoff = d
	}
}

// WithConnectTimeout sets the timeout for initial connection attempts.
func WithConnectTimeout(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.connectTimeout = d
	}
}

// Client represents a websocket client connection with automatic reconnection support.
// It exposes typed read/write channels so callers never touch the raw connection.
type Client[Req, Resp any] struct {
	ClientConfig
	url        *url.URL
	conn       *websocket.Conn
	readChan   chan Resp
	writeChan  chan Req
	errChan    chan error
	close      chan struct{}
	closeMutex sync.RWMutex
	closeErr   error
	cancel     func()
}

// NewClient creates a new websocket client. Call Start to begin processing.
func NewClient[Req, Resp any](rawURL string, options ...ClientOption) (*Client[Req, Resp], error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parsing url %s: %w", rawURL, err)
	}

	config := ClientConfig{
		log:                 slog.Default(),
		dialOptions:         &websocket.DialOptions{},
		httpHeader:          http.Header{},
		readBufSize:         defaultReadBufferSize,
		writeBufSize:        defaultWriteBufferSize,
		connectRetryBackoff: defaultConnectRetryBackoff,
		connectTimeout:      defaultConnectTimeout,
	}

	for _, opt := range options {
		opt(&config)
	}

	client := &Client[Req, Resp]{
		ClientConfig: config,
		url:          parsedURL,
		readChan:     make(chan Resp, config.readBufSize),
		writeChan:    make(chan Req, config.writeBufSize),
		// Buffered to 1 so closeWithError never blocks when sending the first error.
		errChan: make(chan error, 1),
		close:   make(chan struct{}),
	}

	return client, nil
}

// Start establishes the initial connection and spawns background read/write goroutines.
func (c *Client[Req, Resp]) Start(ctx context.Context) error {
	c.log = c.log.WithGroup("websocket_client").With(
		"scheme", c.url.Scheme,
		"host", c.url.Host,
		"path", c.url.Path,
	)
	// Derive a cancellable context so Close() can tear down both goroutines.
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	go c.read(ctx)
	go c.write(ctx)
	return nil
}

// Close gracefully closes the websocket connection.
func (c *Client[Req, Resp]) Close() {
	c.closeWithError(nil)
}

// closeWithError tears down the client exactly once. The sync.RWMutex + closed-channel
// check guarantees idempotency so concurrent callers (read/write goroutines) are safe.
func (c *Client[Req, Resp]) closeWithError(err error) {
	if err != nil {
		c.log.Error("closing", "error", err)
	} else {
		c.log.Info("closing")
	}

	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()

	select {
	case <-c.close:
		// Already closed — nothing to do.
	default:
		c.closeErr = err

		// Cancel the context to unblock any pending conn.Read/conn.Write calls.
		if c.cancel != nil {
			c.cancel()
		}

		// CloseNow performs an immediate close without sending a close frame,
		// matching the original gorilla conn.Close() behaviour.
		if c.conn != nil {
			c.conn.CloseNow()
		}

		close(c.close)

		// Non-blocking send: we only propagate the first error to the caller.
		if err != nil {
			select {
			case c.errChan <- err:
			default:
			}
		}
		close(c.errChan)
	}
}

// Read returns the channel that receives deserialized server messages.
func (c *Client[Req, Resp]) Read() <-chan Resp {
	return c.readChan
}

// Write returns the channel used to send messages to the server.
func (c *Client[Req, Resp]) Write() chan<- Req {
	return c.writeChan
}

// Error returns a channel that receives the first fatal error, if any.
func (c *Client[Req, Resp]) Error() <-chan error {
	return c.errChan
}

// ReadMessage blocks until a message is received, the context is cancelled, or an error occurs.
func (c *Client[Req, Resp]) ReadMessage(ctx context.Context) (Resp, error) {
	var zero Resp

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-c.close:
		if err := c.CloseError(); err != nil {
			return zero, fmt.Errorf("client closed: %w", err)
		}
		return zero, io.EOF
	case err := <-c.errChan:
		return zero, fmt.Errorf("client error: %w", err)
	case msg, ok := <-c.readChan:
		if !ok {
			if err := c.CloseError(); err != nil {
				return zero, fmt.Errorf("read channel closed: %w", err)
			}
			return zero, io.EOF
		}
		return msg, nil
	}
}

// CloseError returns the error that caused the client to close, if any.
func (c *Client[Req, Resp]) CloseError() error {
	c.closeMutex.RLock()
	defer c.closeMutex.RUnlock()
	return c.closeErr
}

// isClosed returns true if the client has been closed.
func (c *Client[Req, Resp]) isClosed() bool {
	c.closeMutex.RLock()
	defer c.closeMutex.RUnlock()
	select {
	case <-c.close:
		return true
	default:
		return false
	}
}

// connect establishes a websocket connection with retry logic. The timeout is enforced
// via a derived context rather than a dialer setting, since coder/websocket controls
// timeouts through context cancellation.
func (c *Client[Req, Resp]) connect(ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()

	// Write-lock because we're mutating c.conn, which the read/write goroutines access
	// under a read-lock.
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()

	if c.conn != nil {
		c.conn.CloseNow()
		c.conn = nil
	}

	// Copy dial options so we can inject the configured HTTP headers without mutating
	// the caller's original options struct.
	dialOptions := c.dialOptions
	if c.httpHeader != nil {
		optsCopy := *dialOptions
		optsCopy.HTTPHeader = c.httpHeader
		dialOptions = &optsCopy
	}

	for {
		select {
		case <-connectCtx.Done():
			return connectCtx.Err()
		default:
		}

		conn, _, err := websocket.Dial(connectCtx, c.url.String(), dialOptions)
		if err != nil {
			c.log.WarnContext(connectCtx, "connection attempt failed, retrying", "error", err)
			time.Sleep(c.connectRetryBackoff)
			continue
		}
		c.conn = conn

		if c.onConnectFN != nil {
			c.onConnectFN(connectCtx)
		}
		c.log.DebugContext(connectCtx, "successfully connected")
		return nil
	}
}

// reconnect attempts to re-establish the websocket connection after a recoverable error.
func (c *Client[Req, Resp]) reconnect(ctx context.Context) error {
	c.log.DebugContext(ctx, "attempting to reconnect")
	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("reconnection failed: %w", err)
	}
	return nil
}

// read continuously reads from the websocket, deserializes JSON messages, and pushes
// them into readChan. On error it either reconnects (for expected/network errors) or
// shuts down the client (for truly unexpected close codes).
func (c *Client[Req, Resp]) read(ctx context.Context) {
	c.log.DebugContext(ctx, "starting read routine")
	defer c.log.DebugContext(ctx, "exiting read routine")
	// Closing readChan signals consumers that no more messages will arrive.
	defer close(c.readChan)

	for {
		if c.isClosed() {
			return
		}

		// Snapshot the current connection under a read-lock. The write-lock is only
		// held during connect/reconnect, so reads don't block each other.
		c.closeMutex.RLock()
		conn := c.conn
		c.closeMutex.RUnlock()

		// coder/websocket's Read is context-aware, so it will unblock when ctx is
		// cancelled (e.g. via Close → cancel).
		_, bytes, err := conn.Read(ctx)
		if err != nil {
			if c.isClosed() {
				c.log.DebugContext(ctx, "read routine exiting due to close")
				return
			}

			// Determine whether this is a recoverable error that should trigger reconnect,
			// or a fatal unexpected close code. This mirrors gorilla's IsUnexpectedCloseError:
			//   - Non-close errors (network failures, context cancellation) → reconnect
			//   - Expected close codes (normal, going away, abnormal) → reconnect
			//   - Any other close code → fatal, shut down client
			closeStatus := websocket.CloseStatus(err)
			isExpected := closeStatus == -1 ||
				closeStatus == websocket.StatusGoingAway ||
				closeStatus == websocket.StatusAbnormalClosure ||
				closeStatus == websocket.StatusNormalClosure
			if !isExpected {
				c.closeWithError(fmt.Errorf("read error: %w", err))
				return
			}

			if err := c.reconnect(ctx); err != nil {
				c.closeWithError(fmt.Errorf("reconnection error: %w", err))
				return
			}
			continue
		}

		var payload Resp
		if err := json.Unmarshal(bytes, &payload); err != nil {
			c.log.ErrorContext(ctx, "failed to unmarshal message", "error", err)
			c.closeWithError(fmt.Errorf("unmarshal error: %w", err))
			return
		}

		// Try a non-blocking send first; if the channel is full, log a warning and
		// then block. This provides backpressure visibility without dropping messages.
		select {
		case c.readChan <- payload:
		case <-ctx.Done():
			return
		case <-c.close:
			return
		default:
			c.log.WarnContext(ctx, "read channel is full - blocking")
			select {
			case c.readChan <- payload:
			case <-ctx.Done():
				return
			case <-c.close:
				return
			}
		}
	}
}

// write drains writeChan and sends each message as JSON over the websocket.
func (c *Client[Req, Resp]) write(ctx context.Context) {
	c.log.DebugContext(ctx, "starting write routine")
	defer c.log.DebugContext(ctx, "exiting write routine")

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.close:
			return
		case payload := <-c.writeChan:
			if c.isClosed() {
				return
			}

			c.closeMutex.RLock()
			conn := c.conn
			c.closeMutex.RUnlock()

			bytes, err := json.Marshal(payload)
			if err != nil {
				c.closeWithError(fmt.Errorf("marshal error: %w", err))
				return
			}

			// coder/websocket's Write is context-aware and will unblock on cancellation.
			if err := conn.Write(ctx, websocket.MessageText, bytes); err != nil {
				if c.isClosed() {
					return
				}
				c.closeWithError(fmt.Errorf("write error: %w", err))
				return
			}
		}
	}
}
