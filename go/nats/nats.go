package nats

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"buf.build/go/protovalidate"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil"
)

type Opts struct {
	Host           string        `long:"host" env:"HOST" required:"true"`
	Port           int           `long:"port" env:"PORT" default:"4222"`
	TotalWait      time.Duration `long:"total-wait" env:"TOTAL_WAIT" default:"10m"`
	ReconnectDelay time.Duration `long:"reconnect-delay" env:"RECONNECT_DELAY" default:"1s"`
	// OutboxPaused stops outbox pollers publishing, so journals buffer events while a cluster is drained.
	OutboxPaused bool `long:"outbox-paused" env:"OUTBOX_PAUSED"`
}

func (o *Opts) url() string {
	return fmt.Sprintf("nats://%s:%d", o.Host, o.Port)
}

// Publisher publishes a message on a subject: a [Client] publishes it now, an outbox journal
// once the transaction writing it commits.
type Publisher interface {
	Publish(ctx context.Context, subject *Subject, message proto.Message) error
}

type Client struct {
	*nats.Conn
	log       *slog.Logger
	opts      *Opts
	JetStream jetstream.JetStream
}

func NewClient(opts *Opts) (*Client, error) {
	return &Client{
		log:  slog.Default(),
		opts: opts,
	}, nil
}

func (c *Client) WithLogger(logger *slog.Logger) *Client {
	c.log = logger
	return c
}

func (c *Client) Start(ctx context.Context) error {
	url := c.opts.url()
	options := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(c.opts.ReconnectDelay),
		nats.MaxReconnects(int(c.opts.TotalWait / c.opts.ReconnectDelay)),
		nats.DisconnectHandler(func(nc *nats.Conn) {
			c.log.Warn(fmt.Sprintf("disconnected: will attempt reconnects for %.0fm", c.opts.TotalWait.Minutes()))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.log.Info(fmt.Sprintf("reconnected [%s]", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if nc.IsClosed() {
				if err := nc.LastError(); err != nil {
					c.log.Warn(fmt.Sprintf("closed handler reason: %v", err))
				}
			} else {
				c.log.Warn(fmt.Sprintf("connection closed reason: %v", nc.LastError()))
			}
		}),
	}

	conn, err := nats.Connect(url, options...)
	if err != nil {
		return fmt.Errorf("connecting to nats: %w", err)
	}
	c.Conn = conn

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("connecting to jetstream: %w", err)
	}
	c.JetStream = js

	c.log.Info(fmt.Sprintf("connected to nats server on [%s]", url))
	return nil
}

// OutboxPaused reports whether outbox pollers must hold their journals rather than publish.
func (c *Client) OutboxPaused() bool {
	return c.opts.OutboxPaused
}

// Encode validates and marshals a message the way it is published.
func Encode(message proto.Message) ([]byte, error) {
	if err := protovalidate.Validate(message); err != nil {
		return nil, fmt.Errorf("validating message: %w", err)
	}
	bytes, err := pbutil.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshaling message: %w", err)
	}
	return bytes, nil
}

func (c *Client) Publish(ctx context.Context, subject *Subject, message proto.Message) error {
	bytes, err := Encode(message)
	if err != nil {
		return err
	}
	if _, err := c.JetStream.Publish(ctx, subject.name, bytes, jetstream.WithExpectStream(subject.stream.name)); err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return nil
}
