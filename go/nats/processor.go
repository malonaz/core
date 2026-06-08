// File: /home/malon/core/go/nats/processor.go

package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/routine"
)

const (
	defaultFetchTimeout = 1 * time.Second
	defaultBatchSize    = 1
	defaultAckWait      = 10 * time.Second
)

type Message[T proto.Message] struct {
	Timestamp time.Time
	Headers   map[string][]string
	Payload   T
	natsMsg   jetstream.Msg
}

// Ack acknowledges the message.
func (m *Message[T]) Ack() error {
	return m.natsMsg.Ack()
}

// Nak negatively acknowledges the message, requesting redelivery.
func (m *Message[T]) Nak() error {
	return m.natsMsg.Nak()
}

type ProcessorFunc[T proto.Message] func(ctx context.Context, message *Message[T]) error

type ProcessorConfig struct {
	Subjects             []*Subject
	ConsumerName         string
	MaxConsecutiveErrors int
	BatchSize            int
	FetchTimeout         time.Duration
	AckWait              time.Duration
	BackoffSeconds       int
	// DeliverFromLatest configures the consumer to start from the latest offset on creation.
	// This only takes effect when the consumer is first created; existing consumers retain their cursor.
	DeliverFromLatest bool
}

type ProcessorOpt[T proto.Message] func(*Processor[T])

// WithGroupKey groups messages by the returned key. Messages within a group are processed
// serially in the order they were received; groups are processed in parallel.
func WithGroupKey[T proto.Message](keyFunc func(*Message[T]) string) ProcessorOpt[T] {
	return func(p *Processor[T]) {
		p.groupKeyFunc = keyFunc
	}
}

type Processor[T proto.Message] struct {
	log           *slog.Logger
	client        *Client
	config        *ProcessorConfig
	processorFunc ProcessorFunc[T]
	groupKeyFunc  func(*Message[T]) string
	consumer      jetstream.Consumer
	routine       *routine.Routine
	metrics       bool
}

func NewProcessor[T proto.Message](client *Client, config *ProcessorConfig, processorFunc ProcessorFunc[T], opts ...ProcessorOpt[T]) *Processor[T] {
	p := &Processor[T]{
		log:           slog.Default(),
		client:        client,
		config:        config,
		processorFunc: processorFunc,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Processor[T]) WithLogger(logger *slog.Logger) *Processor[T] {
	p.log = logger
	return p
}

func (p *Processor[T]) WithMetrics() *Processor[T] {
	p.metrics = true
	return p
}

// HealthCheck checks the health of this routine.
func (p *Processor[T]) HealthCheck(ctx context.Context) error {
	return p.routine.HealthCheck(ctx)
}

func (p *Processor[T]) Start(ctx context.Context) error {
	var stream string
	var filterSubjects []string
	for _, subject := range p.config.Subjects {
		if stream == "" {
			stream = subject.stream.name
		} else {
			if stream != subject.stream.name {
				return fmt.Errorf("all subjects must belong to the same stream")
			}
		}
		filterSubjects = append(filterSubjects, subject.name)
	}

	fetchTimeout := p.config.FetchTimeout
	if fetchTimeout == 0 {
		fetchTimeout = defaultFetchTimeout
	}
	batchSize := p.config.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}
	ackWait := p.config.AckWait
	if ackWait == 0 {
		ackWait = defaultAckWait
	}
	deliverPolicy := jetstream.DeliverAllPolicy
	if p.config.DeliverFromLatest {
		deliverPolicy = jetstream.DeliverNewPolicy
	}
	consumerConfig := jetstream.ConsumerConfig{
		Durable:        p.config.ConsumerName,
		FilterSubjects: filterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  2 * batchSize,
		AckWait:        ackWait,
		DeliverPolicy:  deliverPolicy,
	}
	consumer, err := p.client.JetStream.CreateOrUpdateConsumer(ctx, stream, consumerConfig)
	if err != nil {
		return fmt.Errorf("creating or updating consumer: %w", err)
	}
	p.consumer = consumer

	processFn := func(ctx context.Context) error {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, fetchTimeout)
		defer fetchCancel()
		messageBatch, err := p.consumer.Fetch(batchSize, jetstream.FetchContext(fetchCtx))
		if err != nil {
			return fmt.Errorf("fetching messages: %w", err)
		}

		var messages []*Message[T]
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
				natsMsg:   natsMessage,
			})
		}

		if err := messageBatch.Error(); err != nil {
			return fmt.Errorf("consuming message batch: %w", err)
		}

		if len(messages) == 0 {
			return nil
		}

		ctxWithTimeout, cancel := context.WithTimeout(ctx, ackWait)
		defer cancel()

		// Group messages. Without a groupKeyFunc, each message is its own group.
		var groups [][]*Message[T]
		if p.groupKeyFunc != nil {
			keyToGroupIndex := make(map[string]int)
			for _, message := range messages {
				key := p.groupKeyFunc(message)
				if groupIndex, ok := keyToGroupIndex[key]; ok {
					groups[groupIndex] = append(groups[groupIndex], message)
				} else {
					keyToGroupIndex[key] = len(groups)
					groups = append(groups, []*Message[T]{message})
				}
			}
		} else {
			for _, message := range messages {
				groups = append(groups, []*Message[T]{message})
			}
		}

		// Process groups in parallel; messages within a group serially.
		var mu sync.Mutex
		var errs []error

		var wg sync.WaitGroup
		for _, group := range groups {
			wg.Add(1)
			go func(group []*Message[T]) {
				defer wg.Done()
				for i, message := range group {
					if err := p.processorFunc(ctxWithTimeout, message); err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
						for _, remaining := range group[i:] {
							if nakErr := remaining.Nak(); nakErr != nil {
								p.log.Error("naking message", "error", nakErr)
							}
						}
						return
					}
					if ackErr := message.Ack(); ackErr != nil {
						p.log.Error("acking message", "error", ackErr)
					}
				}
			}(group)
		}
		wg.Wait()

		if len(errs) > 0 {
			return fmt.Errorf("processing messages: %w", errors.Join(errs...))
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
	).WithLogger(p.log).
		WithConstantBackOff(backoffSeconds)

	if p.config.MaxConsecutiveErrors > 0 {
		r = r.WithMaxConsecutiveErrors(p.config.MaxConsecutiveErrors)
	}

	if p.metrics {
		r.WithMetrics()
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
