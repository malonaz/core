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

type StreamConfig = jetstream.StreamConfig
type Stream = jetstream.Stream

type Opts struct {
	Url            string        `long:"url" env:"URL" default:"nats-server:4222"`
	TotalWait      time.Duration `long:"total-wait" env:"TOTAL_WAIT" default:"10m"`
	ReconnectDelay time.Duration `long:"reconnect-delay" env:"RECONNECT_DELAY" default:"1s"`
}

type Client struct {
	*nats.Conn
	log       *slog.Logger
	opts      *Opts
	jetStream jetstream.JetStream
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

	conn, err := nats.Connect(c.opts.Url, options...)
	if err != nil {
		return fmt.Errorf("connecting to nats: %w", err)
	}
	c.Conn = conn

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("connecting to jetstream: %w", err)
	}
	c.jetStream = js

	c.log.Info(fmt.Sprintf("connected to nats server on [%s]", c.opts.Url))
	return nil
}

func (c *Client) Publish(ctx context.Context, subject string, message proto.Message) error {
	if err := protovalidate.Validate(message); err != nil {
		return fmt.Errorf("validating message: %w", err)
	}
	bytes, err := pbutil.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}
	if err := c.Conn.Publish(subject, bytes); err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}
	return nil
}
