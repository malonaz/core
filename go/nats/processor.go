package nats

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/routine"
)

const (
	defaultFetchTimeout = 5 * time.Second
	defaultBatchSize    = 1
	defaultTimeout      = 10 * time.Second
)

type Message[T proto.Message] struct {
	Timestamp time.Time
	Headers   map[string][]string
	Payload   T
}

type ProcessorFunc[T proto.Message] func(ctx context.Context, messages []*Message[T]) error

type ProcessorConfig struct {
	Stream               string
	ConsumerName         string
	FilterSubject        string
	MaxConsecutiveErrors int
	BatchSize            int
	FetchTimeout         time.Duration
	Timeout              time.Duration
	BackoffSeconds       int
}

type Processor[T proto.Message] struct {
	log           *slog.Logger
	client        *Client
	config        *ProcessorConfig
	processorFunc ProcessorFunc[T]
	consumer      jetstream.Consumer
	routine       *routine.Routine
}

func NewProcessor[T proto.Message](client *Client, config *ProcessorConfig, processorFunc ProcessorFunc[T]) *Processor[T] {
	return &Processor[T]{
		log:           slog.Default(),
		client:        client,
		config:        config,
		processorFunc: processorFunc,
	}
}

func (p *Processor[T]) WithLogger(logger *slog.Logger) *Processor[T] {
	p.log = logger
	return p
}

func (p *Processor[T]) Start(ctx context.Context) error {
	fetchTimeout := p.config.FetchTimeout
	if fetchTimeout == 0 {
		fetchTimeout = defaultFetchTimeout
	}
	batchSize := p.config.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}
	timeout := p.config.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	consumerConfig := jetstream.ConsumerConfig{
		Durable:       p.config.ConsumerName,
		FilterSubject: getStreamSubject(p.config.Stream, p.config.FilterSubject),
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 2 * batchSize,
		AckWait:       timeout,
	}
	consumer, err := p.client.JetStream.CreateOrUpdateConsumer(ctx, p.config.Stream, consumerConfig)
	if err != nil {
		return fmt.Errorf("creating or updating consumer: %w", err)
	}
	p.consumer = consumer

	processFn := func(ctx context.Context) error {
		messageBatch, err := p.consumer.Fetch(batchSize, jetstream.FetchMaxWait(fetchTimeout))
		if err != nil {
			return fmt.Errorf("fetching messages: %w", err)
		}

		var messages []*Message[T]
		var natsMessages []jetstream.Msg
		for natsMessage := range messageBatch.Messages() {
			payload := p.newPayload()
			if err := pbutil.Unmarshal(natsMessage.Data(), payload); err != nil {
				if nakErr := natsMessage.Nak(); nakErr != nil {
					p.log.Error("naking message after unmarshal failure", "error", nakErr)
				}
				return fmt.Errorf("unmarshaling payload: %w", err)
			}

			metadata, err := natsMessage.Metadata()
			if err != nil {
				return fmt.Errorf("getting message metadata: %w", err)
			}

			messages = append(messages, &Message[T]{
				Timestamp: metadata.Timestamp,
				Headers:   natsMessage.Headers(),
				Payload:   payload,
			})
			natsMessages = append(natsMessages, natsMessage)
		}

		if err := messageBatch.Error(); err != nil {
			return fmt.Errorf("consuming message batch: %w", err)
		}

		if len(messages) == 0 {
			return nil
		}

		ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		if err := p.processorFunc(ctxWithTimeout, messages); err != nil {
			for _, natsMessage := range natsMessages {
				if nakErr := natsMessage.Nak(); nakErr != nil {
					p.log.Error("naking message after processing failure", "error", nakErr)
				}
			}
			return fmt.Errorf("processing messages: %w", err)
		}

		for _, natsMessage := range natsMessages {
			if err := natsMessage.Ack(); err != nil {
				return fmt.Errorf("acking message: %w", err)
			}
		}
		return nil
	}

	backoffSeconds := p.config.BackoffSeconds
	if backoffSeconds == 0 {
		backoffSeconds = 1
	}

	r := routine.New(
		fmt.Sprintf("nats-processor-%s", p.config.ConsumerName),
		processFn,
		func(err error) { p.log.Error("permanent error", "error", err) },
	).WithLogger(p.log).
		WithConstantBackOff(backoffSeconds)

	if p.config.MaxConsecutiveErrors > 0 {
		r = r.WithMaxConsecutiveErrors(p.config.MaxConsecutiveErrors)
	}

	p.routine = r.Start(ctx)
	return nil
}

func (p *Processor[T]) newPayload() T {
	var payload T
	payload = reflect.New(reflect.TypeOf(payload).Elem()).Interface().(T)
	return payload
}

func (p *Processor[T]) Close() {
	if p.routine != nil {
		p.routine.Close()
	}
}
