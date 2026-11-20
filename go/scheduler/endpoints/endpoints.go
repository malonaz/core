// Package endpoints holds the dispatcher's view of the gRPC endpoints it
// delivers to: one connection per endpoint, and its schema fetched over gRPC
// reflection, from which the scheduler-run methods are discovered.
package endpoints

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	codegenschedulerpb "github.com/malonaz/core/genproto/codegen/scheduler/v1"
	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// schemaTimeout bounds one reflection fetch.
const schemaTimeout = 10 * time.Second

// Method is a scheduler-run method an endpoint serves, as its annotation declares it.
type Method struct {
	Service      string
	Method       string
	RequestType  string
	ResponseType string
	Policy       *policypb.QueuePolicy
}

// Pool caches one connection per endpoint URL.
type Pool struct {
	mutex           sync.Mutex
	urlToConnection map[string]*grpc.Connection
}

// NewPool returns an empty pool.
func NewPool() *Pool {
	return &Pool{urlToConnection: map[string]*grpc.Connection{}}
}

// Acquire returns the connection to the endpoint, dialing it on first use.
func (p *Pool) Acquire(ctx context.Context, url string) (*grpc.Connection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if connection, ok := p.urlToConnection[url]; ok {
		return connection, nil
	}
	connection, err := dial(ctx, url)
	if err != nil {
		return nil, err
	}
	p.urlToConnection[url] = connection
	return connection, nil
}

// Close closes every connection.
func (p *Pool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for url, connection := range p.urlToConnection {
		delete(p.urlToConnection, url)
		connection.Close()
	}
}

// Discover returns the scheduler-run methods the endpoint serves: those
// carrying a malonaz.scheduler.v1.method annotation, read over gRPC
// reflection. The endpoint's message types are registered globally so
// payloads and responses of types this binary does not link render in JSON.
func (p *Pool) Discover(ctx context.Context, url string) ([]*Method, error) {
	connection, err := p.Acquire(ctx, url)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "endpoint %s: %v", url, err).Err()
	}
	ctx, cancel := context.WithTimeout(ctx, schemaTimeout)
	defer cancel()
	files, err := pbreflection.ResolveFiles(ctx, rpb.NewServerReflectionClient(connection.Get()))
	if err != nil {
		if status.HasCode(err, codes.Unimplemented) {
			return nil, status.Errorf(codes.FailedPrecondition, "endpoint %s does not serve gRPC reflection", url).Err()
		}
		return nil, status.FromError(err, "endpoint %s: fetching schema over reflection", url).Err()
	}
	if err := registerGlobalTypes(files); err != nil {
		return nil, status.Errorf(codes.Internal, "endpoint %s: registering types: %v", url, err).Err()
	}
	var methods []*Method
	var rangeErr error
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		services := file.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			serviceMethods := service.Methods()
			for j := 0; j < serviceMethods.Len(); j++ {
				method := serviceMethods.Get(j)
				options, ok := method.Options().(proto.Message)
				if !ok || !proto.HasExtension(options, codegenschedulerpb.E_Method) {
					continue
				}
				methodOptions, err := pbutil.GetExtension[*codegenschedulerpb.MethodOptions](options, codegenschedulerpb.E_Method)
				if err != nil {
					rangeErr = fmt.Errorf("reading %s.%s annotation: %w", service.FullName(), method.Name(), err)
					return false
				}
				methods = append(methods, &Method{
					Service:      string(service.FullName()),
					Method:       string(method.Name()),
					RequestType:  typeURL(method.Input()),
					ResponseType: typeURL(method.Output()),
					Policy:       methodOptions.GetPolicy(),
				})
			}
		}
		return true
	})
	if rangeErr != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "endpoint %s: %v", url, rangeErr).Err()
	}
	return methods, nil
}

// typeURL returns the type URL anypb gives messages of the descriptor.
func typeURL(message protoreflect.MessageDescriptor) string {
	return "type.googleapis.com/" + string(message.FullName())
}

// registerGlobalTypes makes the endpoint's messages resolvable through the
// global type registry, which protojson consults.
func registerGlobalTypes(files *protoregistry.Files) error {
	types, err := pbreflection.NewTypesFromFiles(files)
	if err != nil {
		return err
	}
	var registrationErr error
	types.RangeMessages(func(messageType protoreflect.MessageType) bool {
		if _, err := protoregistry.GlobalTypes.FindMessageByName(messageType.Descriptor().FullName()); err == nil {
			return true
		}
		registrationErr = protoregistry.GlobalTypes.RegisterMessage(messageType)
		return registrationErr == nil
	})
	return registrationErr
}

func dial(ctx context.Context, url string) (*grpc.Connection, error) {
	opts, err := grpc.ParseClientOpts(url)
	if err != nil {
		return nil, fmt.Errorf("parsing url %q: %w", url, err)
	}
	connection, err := grpc.NewConnection(opts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}
	// The dial is non-blocking; the connection outlives the call that opened it.
	if err := connection.Connect(context.WithoutCancel(ctx)); err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	return connection, nil
}
