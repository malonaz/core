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

	"github.com/go-json-experiment/json"
	"github.com/gorilla/websocket"
)

const (
	defaultConnectRetryBackoff = 2 * time.Second
	defaultConnectTimeout      = 10 * time.Second
)

// ClientConfig holds the configuration options for a Client
type ClientConfig struct {
	log                 *slog.Logger
	onConnectFN         func(context.Context)
	httpHeader          http.Header
	dialer              *websocket.Dialer
	readBufSize         int
	writeBufSize        int
	connectRetryBackoff time.Duration
	connectTimeout      time.Duration
}

// ClientOption configures a Client
type ClientOption func(*ClientConfig)

// WithClientLogger sets the logger
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *ClientConfig) {
		c.log = logger
	}
}

// WithOnConnect sets a callback function called when the websocket is connected (or reconnected)
func WithOnConnect(fn func(context.Context)) ClientOption {
	return func(c *ClientConfig) {
		c.onConnectFN = fn
	}
}

// WithHTTPHeader sets custom HTTP headers for the websocket upgrade request
func WithHTTPHeader(header http.Header) ClientOption {
	return func(c *ClientConfig) {
		c.httpHeader = header
	}
}

// WithDialer sets a custom websocket dialer
func WithDialer(dialer *websocket.Dialer) ClientOption {
	return func(c *ClientConfig) {
		c.dialer = dialer
	}
}

// WithClientReadBufferSize sets the size of the read channel buffer
func WithClientReadBufferSize(size int) ClientOption {
	return func(c *ClientConfig) {
		c.readBufSize = size
	}
}

// WithClientWriteBufferSize sets the size of the write channel buffer
func WithClientWriteBufferSize(size int) ClientOption {
	return func(c *ClientConfig) {
		c.writeBufSize = size
	}
}

// WithConnectRetryBackoff sets the backoff duration between connection retry attempts
func WithConnectRetryBackoff(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.connectRetryBackoff = d
	}
}

// WithConnectTimeout sets the timeout for initial connection attempts
func WithConnectTimeout(d time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.connectTimeout = d
	}
}

// Client represents a websocket client connection with automatic reconnection support
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

// NewClient creates a new websocket client
func NewClient[Req, Resp any](rawURL string, options ...ClientOption) (*Client[Req, Resp], error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parsing url %s: %w", rawURL, err)
	}

	config := ClientConfig{
		log: slog.Default(),
		dialer: &websocket.Dialer{
			HandshakeTimeout: defaultConnectTimeout,
		},
		httpHeader:          http.Header{},
		readBufSize:         defaultReadBufferSize,
		writeBufSize:        defaultWriteBufferSize,
		connectRetryBackoff: defaultConnectRetryBackoff,
		connectTimeout:      defaultConnectTimeout,
	}

	// Apply options
	for _, opt := range options {
		opt(&config)
	}

	client := &Client[Req, Resp]{
		ClientConfig: config,
		url:          parsedURL,
		readChan:     make(chan Resp, config.readBufSize),
		writeChan:    make(chan Req, config.writeBufSize),
		errChan:      make(chan error, 1),
		close:        make(chan struct{}),
	}

	return client, nil
}

// Start begins processing reads/writes from/to the websocket connection
func (c *Client[Req, Resp]) Start(ctx context.Context) error {
	c.log = c.log.WithGroup("websocket_client").With(
		"scheme", c.url.Scheme,
		"host", c.url.Host,
		"path", c.url.Path,
	)
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	go c.read(ctx)
	go c.write(ctx)
	return nil
}

// Close gracefully closes the websocket connection
func (c *Client[Req, Resp]) Close() {
	c.closeWithError(nil)
}

// closeWithError closes the client with an error
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
		// Already closed
	default:
		c.closeErr = err

		if c.cancel != nil {
			c.cancel()
		}

		if c.conn != nil {
			c.conn.Close()
		}

		close(c.close)

		// Send error if provided (non-blocking)
		if err != nil {
			select {
			case c.errChan <- err:
			default:
				// We only send the first error up to the caller
			}
		}
		close(c.errChan)
	}
}

// Read returns the read channel
func (c *Client[Req, Resp]) Read() <-chan Resp {
	return c.readChan
}

// Write returns the write channel
func (c *Client[Req, Resp]) Write() chan<- Req {
	return c.writeChan
}

// Error returns the error channel
func (c *Client[Req, Resp]) Error() <-chan error {
	return c.errChan
}

// ReadMessage blocks until a message is received, the context is cancelled, or an error occurs
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

// CloseError returns the error that caused the client to close, if any
func (c *Client[Req, Resp]) CloseError() error {
	c.closeMutex.RLock()
	defer c.closeMutex.RUnlock()
	return c.closeErr
}

// isClosed returns true if the client has been closed
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

// connect establishes a websocket connection with retry logic
func (c *Client[Req, Resp]) connect(ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()

	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	for {
		select {
		case <-connectCtx.Done():
			return connectCtx.Err()
		default:
		}

		conn, _, err := c.dialer.DialContext(connectCtx, c.url.String(), c.httpHeader)
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

// reconnect attempts to reconnect to the websocket server
func (c *Client[Req, Resp]) reconnect(ctx context.Context) error {
	c.log.DebugContext(ctx, "attempting to reconnect")
	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("reconnection failed: %w", err)
	}
	return nil
}

func (c *Client[Req, Resp]) read(ctx context.Context) {
	c.log.DebugContext(ctx, "starting read routine")
	defer c.log.DebugContext(ctx, "exiting read routine")
	defer close(c.readChan)

	for {
		if c.isClosed() {
			return
		}

		c.closeMutex.RLock()
		conn := c.conn
		c.closeMutex.RUnlock()

		_, bytes, err := conn.ReadMessage()
		if err != nil {
			if c.isClosed() {
				c.log.DebugContext(ctx, "read routine exiting due to close")
				return
			}

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				c.closeWithError(fmt.Errorf("read error: %w", err))
				return
			}

			// Attempt to reconnect
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

			if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
				if c.isClosed() {
					return
				}
				c.closeWithError(fmt.Errorf("write error: %w", err))
				return
			}
		}
	}
}
