package scheduler_service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// schemaTimeout bounds one reflection fetch; a target that does not answer
// fails the queue mutation rather than holding it to the client's deadline.
const schemaTimeout = 10 * time.Second

// targetSchema is what a target served, as of the target's etag.
type targetSchema struct {
	etag  string
	files *protoregistry.Files
}

// targetSchemas caches each target's schema, keyed by the target's etag so an
// update refetches. A method missing from a cached schema refetches it once:
// the target may have been redeployed with new methods behind the same url.
type targetSchemas struct {
	mutex        sync.Mutex
	nameToSchema map[string]*targetSchema
}

func newTargetSchemas() *targetSchemas {
	return &targetSchemas{nameToSchema: map[string]*targetSchema{}}
}

// get returns the target's cached schema and whether it was cached; a stale
// or unknown target is fetched.
func (c *targetSchemas) get(ctx context.Context, target *schedulerpb.Target, reflectionClient rpb.ServerReflectionClient) (*protoregistry.Files, bool, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if current, ok := c.nameToSchema[target.GetName()]; ok && current.etag == target.GetEtag() {
		return current.files, true, nil
	}
	files, err := c.fetch(ctx, target, reflectionClient)
	return files, false, err
}

// refresh refetches the target's schema regardless of what is cached.
func (c *targetSchemas) refresh(ctx context.Context, target *schedulerpb.Target, reflectionClient rpb.ServerReflectionClient) (*protoregistry.Files, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.fetch(ctx, target, reflectionClient)
}

// fetch must be called with the mutex held: the fetch itself is serialized so
// concurrent mutations on one target share a single round-trip.
func (c *targetSchemas) fetch(ctx context.Context, target *schedulerpb.Target, reflectionClient rpb.ServerReflectionClient) (*protoregistry.Files, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaTimeout)
	defer cancel()
	files, err := pbreflection.ResolveFiles(ctx, reflectionClient)
	if err != nil {
		if status.HasCode(err, codes.Unimplemented) {
			return nil, status.Errorf(codes.FailedPrecondition, "target %s does not serve gRPC reflection", target.GetName()).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "target %s: fetching schema over reflection: %v", target.GetName(), err).Err()
	}
	if err := registerGlobalTypes(files); err != nil {
		return nil, status.Errorf(codes.Internal, "target %s: registering types: %v", target.GetName(), err).Err()
	}
	c.nameToSchema[target.GetName()] = &targetSchema{etag: target.GetEtag(), files: files}
	return files, nil
}

// evict forgets a deleted target's schema.
func (c *targetSchemas) evict(name string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.nameToSchema, name)
}

// registerGlobalTypes makes the target's messages resolvable through the
// global type registry, which protojson consults: without it, payloads and
// responses of types this binary does not link would render as opaque Any.
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

// resolveMethod returns the descriptor of a "/package.Service/Method" gRPC
// method as the target serves it.
func (s *Service) resolveMethod(ctx context.Context, target *schedulerpb.Target, method string) (protoreflect.MethodDescriptor, error) {
	connection, release, err := s.targets.acquire(ctx, target)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "target %s: %v", target.GetName(), err).Err()
	}
	defer release()
	reflectionClient := rpb.NewServerReflectionClient(connection.Get())
	ctx = outgoingContext(ctx, target)

	files, cached, err := s.schemas.get(ctx, target, reflectionClient)
	if err != nil {
		return nil, err
	}
	methodDescriptor, err := findMethod(files, method)
	if err != nil && cached {
		if files, err = s.schemas.refresh(ctx, target, reflectionClient); err != nil {
			return nil, err
		}
		methodDescriptor, err = findMethod(files, method)
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "handler %s: %v", method, err).Err()
	}
	return methodDescriptor, nil
}

func findMethod(files *protoregistry.Files, method string) (protoreflect.MethodDescriptor, error) {
	fullName := protoreflect.FullName(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "."))
	descriptor, err := files.FindDescriptorByName(fullName)
	if err != nil {
		return nil, fmt.Errorf("method %q is not served by the target", method)
	}
	methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a method", method)
	}
	return methodDescriptor, nil
}
