package cartesia

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/malonaz/core/go/websocket"
)

type MultiplexedRequest[RequestType any] struct {
	ContextID string      `json:"context_id"`
	Request   RequestType `json:",inline"`
}

type MultiplexedResponse[ResponseType any] struct {
	ContextID string       `json:"context_id"`
	Response  ResponseType `json:",inline"`
}

type Stream[RequestType any, ResponseType any] struct {
	contextID   string
	eventChan   chan ResponseType
	errChan     chan error
	done        chan struct{}
	multiplexer *Multiplexer[RequestType, ResponseType]
}

func (s *Stream[RequestType, ResponseType]) Send(request RequestType) error {
	multiplexedRequest := MultiplexedRequest[RequestType]{
		ContextID: s.contextID,
		Request:   request,
	}

	select {
	case <-s.done:
		return fmt.Errorf("stream is closed")
	case s.multiplexer.websocketClient.Write() <- multiplexedRequest:
		return nil
	}
}

func (s *Stream[RequestType, ResponseType]) Recv(ctx context.Context) (ResponseType, error) {
	select {
	case <-ctx.Done():
		var zero ResponseType
		return zero, ctx.Err()
	case <-s.done:
		var zero ResponseType
		return zero, fmt.Errorf("stream is closed")
	case event := <-s.eventChan:
		return event, nil
	case err := <-s.errChan:
		var zero ResponseType
		return zero, err
	}
}

func (s *Stream[RequestType, ResponseType]) Close() error {
	select {
	case <-s.done:
		return nil // Already closed
	default:
		close(s.done)
	}

	// Send cancel request
	// Note: We need to send a cancel request which doesn't follow the normal RequestType
	// We'll need to handle this specially or create a wrapper type
	cancelRequest := MultiplexedRequest[RequestType]{
		ContextID: s.contextID,
		// Note: This is a bit awkward - the cancel field doesn't fit into RequestType
		// You may need to adjust the API or use a different approach for cancellation
	}

	select {
	case s.multiplexer.websocketClient.Write() <- cancelRequest:
	case <-s.multiplexer.ctx.Done():
		return s.multiplexer.ctx.Err()
	}

	// Remove stream from multiplexer
	s.multiplexer.mutex.Lock()
	delete(s.multiplexer.contextIDToStream, s.contextID)
	s.multiplexer.mutex.Unlock()

	return nil
}

type Multiplexer[RequestType any, ResponseType any] struct {
	contextIDNext     atomic.Int64
	contextIDToStream map[string]*Stream[RequestType, ResponseType]
	mutex             sync.RWMutex
	websocketClient   *websocket.Client[MultiplexedRequest[RequestType], MultiplexedResponse[ResponseType]]
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewMultiplexer[RequestType any, ResponseType any](
	websocketClient *websocket.Client[MultiplexedRequest[RequestType], MultiplexedResponse[ResponseType]],
) *Multiplexer[RequestType, ResponseType] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Multiplexer[RequestType, ResponseType]{
		contextIDToStream: map[string]*Stream[RequestType, ResponseType]{},
		websocketClient:   websocketClient,
		ctx:               ctx,
		cancel:            cancel,
	}
}

func (m *Multiplexer[RequestType, ResponseType]) NewStream() *Stream[RequestType, ResponseType] {
	contextID := strconv.FormatInt(m.contextIDNext.Add(1), 10)
	stream := &Stream[RequestType, ResponseType]{
		contextID:   contextID,
		eventChan:   make(chan ResponseType, 10),
		errChan:     make(chan error, 1),
		done:        make(chan struct{}),
		multiplexer: m,
	}
	m.mutex.Lock()
	m.contextIDToStream[contextID] = stream
	m.mutex.Unlock()
	return stream
}

func (m *Multiplexer[RequestType, ResponseType]) Close() {
	m.cancel()
	m.websocketClient.Close()
}

func (m *Multiplexer[RequestType, ResponseType]) Multiplex(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.ctx.Done():
			return m.ctx.Err()
		case err := <-m.websocketClient.Error():
			// Forward error to all active streams
			m.mutex.RLock()
			for _, stream := range m.contextIDToStream {
				select {
				case stream.errChan <- err:
				default:
				}
			}
			m.mutex.RUnlock()
			return err
		case multiplexedResponse := <-m.websocketClient.Read():
			if multiplexedResponse.ContextID == "" {
				continue
			}

			// Find the stream
			m.mutex.RLock()
			stream, exists := m.contextIDToStream[multiplexedResponse.ContextID]
			m.mutex.RUnlock()

			if !exists {
				continue
			}

			select {
			case stream.eventChan <- multiplexedResponse.Response:
			case <-stream.done:
				// Stream was closed
			case <-ctx.Done():
				return ctx.Err()
			case <-m.ctx.Done():
				return m.ctx.Err()
			}
		}
	}
}
