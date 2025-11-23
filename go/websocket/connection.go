package websocket

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

const (
	connectRetryBackoff = 2 * time.Second
	readBufferSize      = 10000
	writeBufferSize     = 10000
)

// UnmarshalFN is syntactic sugar for an unmarshal function, required to be set for every Connection.
type UnmarshalFN func([]byte) (interface{}, error)

// OnConnectFN is syntactic sugar for an on connect function.
type OnConnectFN func(context.Context, *Connection) error

// Connection represents a websocket connection, exposing buffered write/read channels
// to allow for concurrent processing whilst protectice access to the non-thread safe
// Read/Write methods.
type Connection struct {
	conn        *websocket.Conn
	url         *url.URL
	httpHeader  http.Header
	onConnectFN OnConnectFN
	unmarshalFN UnmarshalFN
	readChan    chan interface{}
	writeChan   chan interface{}
	mutex       sync.RWMutex
	closed      bool
	cancel      func()
}

// NewConnection instantiates and returns a new Connection.
func NewConnection(rawURL string) (*Connection, error) {
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parsing url %s: %w", rawURL, err)
	}
	httpHeader := http.Header{}
	if url.User != nil {
		httpHeader.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(url.User.String())))
		url.User = nil
	}
	connection := &Connection{
		url:        url,
		httpHeader: httpHeader,
		readChan:   make(chan interface{}, readBufferSize),
		writeChan:  make(chan interface{}, writeBufferSize),
	}
	return connection, nil
}

// MustNewConnection calls NewConnection and panics on error.
func MustNewConnection(rawURL string) *Connection {
	connection, err := NewConnection(rawURL)
	if err != nil {
		log.Panic(err)
	}
	return connection
}

// Close gracefully closes this websocket connection.
func (c *Connection) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cancel()
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
	c.closed = true
}

// Returns true if this Connection has been closed a caller.
func (c *Connection) isClosed() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.closed
}

// SetOnConnectFN allows you to set an call back when the websocket is connected (or reconnected).
func (c *Connection) SetOnConnectFN(fn OnConnectFN) { c.onConnectFN = fn }

// Start processing reads/writes from/to the websocket connection.
func (c *Connection) Start(ctx context.Context, timeoutSeconds int64, unmarshalFN UnmarshalFN) error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	if err := c.connect(ctxWithTimeout); err != nil {
		return fmt.Errorf("dialing connection to %s: %w", c.url.String(), err)
	}
	ctx, cancel = context.WithCancel(ctx)
	c.unmarshalFN = unmarshalFN
	c.cancel = cancel
	go c.read(ctx)
	go c.write(ctx)
	return nil
}

// Read returns the read channel.
func (c *Connection) Read() <-chan interface{} { return c.readChan }

// Write returns the write channel.
func (c *Connection) Write() chan<- interface{} { return c.writeChan }

func (c *Connection) connect(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var err error
		c.conn, _, err = websocket.DefaultDialer.DialContext(ctx, c.url.String(), c.httpHeader)
		if err != nil {
			log.Warningf("failure to dial %s - retrying: %v", c.url.String(), err)
			time.Sleep(connectRetryBackoff)
			continue
		}
		if c.onConnectFN != nil {
			if err := c.onConnectFN(ctx, c); err != nil {
				log.Errorf("Failed to execute on connect fn - retrying: %v", err)
				c.conn.Close()
				c.conn = nil
				continue
			}
		}
		break
	}
	log.Infof("successfully connected to %s", c.url.String())
	return nil
}

func (c *Connection) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("exiting write routine for websocket: %s", c.url.String())
			return
		case payload := <-c.writeChan:
			c.mutex.RLock()
			conn := c.conn
			c.mutex.RUnlock()

			if err := conn.WriteJSON(payload); err != nil {
				// If a caller has called `Closed`, we do want to spam with an error message. We simply exit.
				if c.isClosed() {
					log.Infof("exiting write routine for websocket: %s", c.url.String())
					return
				}

				log.Errorf("writing to websocket %s: %v", c.url.String(), err)
				log.Warningf("reconnecting to %s", c.url.String())
				c.connect(ctx)
			}
		}
	}
}

func (c *Connection) read(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("exiting read routine for websocket: %s", c.url.String())
			return
		default:
		}

		c.mutex.RLock()
		conn := c.conn
		c.mutex.RUnlock()

		_, bytes, err := conn.ReadMessage()
		if err != nil {
			// If a caller has called `Closed`, we do want to spam with an error message. We simply exit.
			if c.isClosed() {
				log.Infof("exiting read routine for websocket: %s", c.url.String())
				return
			}
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("reading message from %s: %v", c.url.String(), err)
			}
			log.Warningf("reconnecting to %s", c.url.String())
			c.connect(ctx)
			continue
		}

		payload, err := c.unmarshalFN(bytes)
		if err != nil {
			log.Errorf("unmarshaling message from %s: %v", c.url.String(), err)
			continue
		}
		select {
		case c.readChan <- payload:
		default:
			log.Warningf("read channel for websocket %s is full", c.url.String())
			c.readChan <- payload // Wait for buffer to free up.
		}
	}
}
