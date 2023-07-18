package operation

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"common/go/logging"
	pb "common/go/operation/proto/api"
)

var log = logging.NewLogger()

// LongrunningFN is the interface that a longrunning operation's function must implement.
type LongrunningFN func(context.Context, *Operation) (proto.Message, error)

// Manager implements the longrunning.Operations service. It is designed to be embedded in services that own longrunning operations.
type Manager struct {
	deleteSuccessfulOperations bool
	operationIDToOperation     map[string]*Operation
	concurrentOperationBarrier chan struct{}
	mutex                      sync.Mutex
}

// NewManager instantiates and returns a new manager.
func NewManager() *Manager {
	return &Manager{
		operationIDToOperation: map[string]*Operation{},
	}
}

// WithDeleteSuccessfulOperations activates the deletion of successful operations.
func (m *Manager) WithDeleteSuccessfulOperations() *Manager {
	m.deleteSuccessfulOperations = true
	return m
}

// WithMaxConcurrentOperations limits the number of concurrent operations.
func (m *Manager) WithMaxConcurrentOperations(n int) *Manager {
	m.concurrentOperationBarrier = make(chan struct{}, n)
	return m
}

// GetOperation returns the given operation if it exists.
func (m *Manager) GetOperation(ctx context.Context, request *longrunning.GetOperationRequest) (*longrunning.Operation, error) {
	m.mutex.Lock()
	operation, ok := m.operationIDToOperation[request.Name]
	m.mutex.Unlock()
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "operation does not exist")
	}
	return operation.Proto(), nil
}

// DeleteOperation deletes the given operation.
func (m *Manager) DeleteOperation(ctx context.Context, request *longrunning.DeleteOperationRequest) (*emptypb.Empty, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, ok := m.operationIDToOperation[request.Name]; !ok {
		return nil, grpcstatus.Error(codes.NotFound, "operation does not exist")
	}
	delete(m.operationIDToOperation, request.Name)
	return &emptypb.Empty{}, nil
}

// WaitOperation waits until the operation is done or the context is cancelled.
func (m *Manager) WaitOperation(ctx context.Context, request *longrunning.WaitOperationRequest) (*longrunning.Operation, error) {
	// If request timeout is smaller than request timeout, use request timeout.
	if request.Timeout != nil {
		if err := request.Timeout.CheckValid; err != nil {
			return nil, grpcstatus.Error(codes.InvalidArgument, "invalid duration")
		}
		duration := request.Timeout.AsDuration()
		requestDeadline := time.Now().Add(duration)
		ctxDeadline, ok := ctx.Deadline()
		if !ok || requestDeadline.Before(ctxDeadline) {
			var cancel func()
			ctx, cancel = context.WithDeadline(ctx, requestDeadline)
			defer cancel()
		}
	}

	m.mutex.Lock()
	operation, ok := m.operationIDToOperation[request.Name]
	m.mutex.Unlock()
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "operation does not exist")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-operation.done:
		return operation.Proto(), nil
	}
}

// ListOperations lists operations.
func (m *Manager) ListOperations(ctx context.Context, request *longrunning.ListOperationsRequest) (*longrunning.ListOperationsResponse, error) {
	m.mutex.Lock()
	operations := make([]*Operation, 0, len(m.operationIDToOperation))
	for _, operation := range m.operationIDToOperation {
		operations = append(operations, operation)
	}
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].metadata.CreationTimestamp > operations[j].metadata.CreationTimestamp
	})
	operationPBs := make([]*longrunning.Operation, 0, len(m.operationIDToOperation))
	for _, operation := range operations {
		operationPBs = append(operationPBs, proto.Clone(operation.Proto()).(*longrunning.Operation))
	}
	m.mutex.Unlock()
	return &longrunning.ListOperationsResponse{Operations: operationPBs}, nil
}

// CancelOperation cancels an operation if it exists.
func (m *Manager) CancelOperation(ctx context.Context, request *longrunning.CancelOperationRequest) (*emptypb.Empty, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	operation, ok := m.operationIDToOperation[request.Name]
	if !ok {
		return nil, grpcstatus.Error(codes.NotFound, "operation does not exist")
	}
	operation.cancel()
	return &emptypb.Empty{}, nil
}

// CreateOperation instantiates and returns a new operation.
func (m *Manager) CreateOperation(id string, request proto.Message, fn LongrunningFN, timeout time.Duration) (*Operation, error) {
	log := log.WithField("operation_id", id)
	requestAny, err := anypb.New(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling request to anypb.Any")
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	operation, ok := m.operationIDToOperation[id]
	if ok {
		return operation, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	operation = &Operation{
		id:     id,
		done:   make(chan struct{}),
		cancel: cancel,
		metadata: &pb.Metadata{
			CreationTimestamp: uint64(time.Now().UnixMicro()),
			Request:           requestAny,
			Status:            pb.Status_STATUS_QUEUED,
		},
	}
	m.operationIDToOperation[id] = operation
	go func() {
		defer operation.cancel()
		// Queue the transaction if necessary.
		log.Infof("longrunning operation with id %s queued", id)

		if m.concurrentOperationBarrier != nil {
			m.concurrentOperationBarrier <- struct{}{}
		}
		log.Infof("longrunning operation with id %s started", id)
		operation.mutex.Lock()
		operation.metadata.Status = pb.Status_STATUS_PROCESSING
		operation.mutex.Unlock()
		response, err := fn(ctx, operation)
		if m.concurrentOperationBarrier != nil {
			<-m.concurrentOperationBarrier // Release the barrier.
		}
		var responseAny *anypb.Any
		if err == nil {
			responseAny, err = anypb.New(response)
		}
		if err != nil {
			s, ok := grpcstatus.FromError(err)
			if !ok {
				// Check if this is a context error.
				s = grpcstatus.FromContextError(err)
			}
			status := &status.Status{
				Code:    int32(s.Code()),
				Message: s.Message(),
			}
			operation.mutex.Lock()
			operationLogs := strings.Join(operation.metadata.Logs, ",")
			operation.status = status
			operation.metadata.Status = pb.Status_STATUS_COMPLETED
			operation.metadata.CompletionTimestamp = uint64(time.Now().UnixMicro())
			close(operation.done)
			operation.mutex.Unlock()
			log.WithField("operation_logs", operationLogs).Errorf("longrunning operation with id %s failed: %v", id, err)
			return
		}

		log.Infof("longrunning operation with id %s completed successfully", id)
		operation.mutex.Lock()
		operation.response = responseAny
		operation.metadata.Status = pb.Status_STATUS_COMPLETED
		operation.metadata.CompletionTimestamp = uint64(time.Now().UnixMicro())
		close(operation.done)
		operation.mutex.Unlock()
		if m.deleteSuccessfulOperations {
			m.DeleteOperation(ctx, &longrunning.DeleteOperationRequest{Name: operation.id})
		}
	}()
	return operation, nil
}

// Operation represents a longrunning operation.
type Operation struct {
	id     string
	done   chan struct{}
	cancel func()
	mutex  sync.Mutex

	// Result of this operation.
	metadata *pb.Metadata
	response *anypb.Any
	status   *status.Status
}

// Log adds a log to the metadata of this operation.
func (o *Operation) Log(message string, args ...any) {
	logs := []string{}
	for _, str := range strings.Split(fmt.Sprintf(fmt.Sprintf("%s - %s", time.Now().UTC().Format(time.StampNano), message), args...), "\n") {
		logs = append(logs, str)
	}
	o.mutex.Lock()
	o.metadata.Logs = append(o.metadata.Logs, logs...)
	o.mutex.Unlock()
}

// GetMetadata returns this operation's metadata.
func (o *Operation) GetMetadata() *pb.Metadata {
	o.mutex.Lock()
	metadata := proto.Clone(o.metadata).(*pb.Metadata)
	o.mutex.Unlock()
	return metadata
}

// Proto returns the longrunningpb.Operation representation of this operation.
func (o *Operation) Proto() *longrunning.Operation {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	done := false
	select {
	case <-o.done:
		done = true
	default:
	}
	longrunningOperation := &longrunning.Operation{Name: o.id, Done: done}
	metadata, err := anypb.New(o.metadata)
	if err != nil {
		status := &status.Status{
			Code:    int32(codes.Internal),
			Message: fmt.Sprintf("converting metadata to anypb.Any: %v", err),
		}
		longrunningOperation.Result = &longrunning.Operation_Error{Error: status}
		return longrunningOperation
	}
	longrunningOperation.Metadata = metadata

	if !done {
		return longrunningOperation
	}
	if o.status != nil {
		longrunningOperation.Result = &longrunning.Operation_Error{Error: proto.Clone(o.status).(*status.Status)}
		return longrunningOperation
	}
	longrunningOperation.Result = &longrunning.Operation_Response{Response: proto.Clone(o.response).(*anypb.Any)}
	return longrunningOperation
}

// ToLongrunningOperation wraps a response object into a longrunning.Operation object.
func ToLongrunningOperation(operationID string, request, response proto.Message, creationTimestamp uint64) (*longrunning.Operation, error) {
	requestAny, err := anypb.New(request)
	if err != nil {
		return nil, errors.Wrap(err, "converting request")
	}
	responseAny, err := anypb.New(response)
	if err != nil {
		return nil, errors.Wrap(err, "converting response")
	}
	metadata := &pb.Metadata{
		CreationTimestamp: creationTimestamp,
		Request:           requestAny,
		Status:            pb.Status_STATUS_COMPLETED,
	}
	metadataAny, err := anypb.New(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "converting metadata")
	}
	return &longrunning.Operation{
		Name:     operationID,
		Done:     true,
		Metadata: metadataAny,
		Result: &longrunning.Operation_Response{
			Response: responseAny,
		},
	}, nil
}
