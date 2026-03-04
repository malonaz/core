package middleware

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"

	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
	"github.com/malonaz/core/go/pbutil"
)

// HookHandler is a typed function that processes a proto message when a hook is triggered.
// The handler receives the context and the specific proto message type it was registered for.
type HookHandler[T proto.Message] func(ctx context.Context, message T) error

// HookMatcher determines whether a hook should be invoked for a given RPC.
// It receives the context and call metadata containing the service name, method name, and RPC type.
// Returning true means the hook will be invoked; false means it will be skipped.
type HookMatcher func(ctx context.Context, callMetadata *CallMetadata) bool

// HookOption configures optional behavior for a hook registration.
type HookOption func(*hookRegistration)

// WithMatcher sets a matcher function on a hook registration.
// When set, the hook will only be invoked if the matcher returns true for the current RPC.
// If no matcher is set, the hook is invoked for all RPCs that contain the annotated message type.
func WithMatcher(matcher HookMatcher) HookOption {
	return func(r *hookRegistration) {
		r.matcher = matcher
	}
}

// hookRegistration stores a single hook handler along with its optional matcher.
// The handler accepts a generic proto.Message since the type-specific wrapper is
// created at registration time via RegisterHookHandler.
type hookRegistration struct {
	handler func(context.Context, proto.Message) error
	matcher HookMatcher
}

// hookRegistry is a thread-safe registry mapping hook keys to their registered handlers.
// Hook keys are defined in proto message options via the grpc.hook annotation.
type hookRegistry struct {
	mu                     sync.RWMutex
	hookKeyToRegistrations map[string][]*hookRegistration
}

// globalHookRegistry is the singleton registry used by all hook interceptors.
// Handlers are registered at init time via RegisterHookHandler and looked up at request time.
var globalHookRegistry = &hookRegistry{
	hookKeyToRegistrations: make(map[string][]*hookRegistration),
}

// RegisterHookHandler registers a typed hook handler for a given hook key.
// The hook key corresponds to the key field in the grpc.HookOptions proto annotation.
// The generic type parameter T ensures type safety: the handler will only be called
// with messages of the correct type. This function is typically called during init().
func RegisterHookHandler[T proto.Message](hookKey string, handler HookHandler[T], opts ...HookOption) {
	registration := &hookRegistration{
		// Wrap the typed handler in a generic proto.Message handler.
		// The type assertion is safe because the hook system only invokes handlers
		// with messages whose proto type matches the annotated message.
		handler: func(ctx context.Context, message proto.Message) error {
			return handler(ctx, message.(T))
		},
	}
	for _, opt := range opts {
		opt(registration)
	}
	globalHookRegistry.mu.Lock()
	defer globalHookRegistry.mu.Unlock()
	globalHookRegistry.hookKeyToRegistrations[hookKey] = append(globalHookRegistry.hookKeyToRegistrations[hookKey], registration)
}

// UnaryServerHook returns a unary server interceptor that invokes hooks on request and response messages.
// For requests, hooks annotated with on_request=true are invoked before the handler.
// For responses, hooks annotated with on_response=true are invoked after the handler succeeds.
func UnaryServerHook() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Build call metadata once for this RPC, used by all hook matchers.
		callMetadata := newServerCallMetadata(info.FullMethod, nil, req)

		// Invoke request hooks before calling the handler.
		if message, ok := req.(proto.Message); ok {
			if err := invokeHooks(ctx, callMetadata, message, true); err != nil {
				return nil, err
			}
		}

		response, err := handler(ctx, req)
		if err != nil {
			return response, err
		}

		// Invoke response hooks after the handler succeeds.
		if message, ok := response.(proto.Message); ok {
			if err := invokeHooks(ctx, callMetadata, message, false); err != nil {
				return nil, err
			}
		}
		return response, err
	}
}

// StreamServerHook returns a stream server interceptor that invokes hooks on streamed messages.
// Request hooks are invoked on received messages; response hooks are invoked on sent messages.
func StreamServerHook() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Build call metadata for this stream RPC.
		callMetadata := newServerCallMetadata(info.FullMethod, info, nil)
		return handler(srv, &hookServerStream{
			ServerStream: stream,
			callMetadata: callMetadata,
		})
	}
}

// hookServerStream wraps a grpc.ServerStream to intercept sent and received messages
// and invoke the appropriate hooks on them.
type hookServerStream struct {
	grpc.ServerStream
	callMetadata *CallMetadata
}

// SendMsg intercepts outgoing stream messages and invokes response hooks before sending.
func (s *hookServerStream) SendMsg(m any) error {
	if message, ok := m.(proto.Message); ok {
		if err := invokeHooks(s.Context(), s.callMetadata, message, false); err != nil {
			return err
		}
	}
	return s.ServerStream.SendMsg(m)
}

// RecvMsg intercepts incoming stream messages and invokes request hooks after receiving.
func (s *hookServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if message, ok := m.(proto.Message); ok {
		if err := invokeHooks(s.Context(), s.callMetadata, message, true); err != nil {
			return err
		}
	}
	return nil
}

// invokeHooks walks the proto message tree using protorange and invokes registered hooks
// for any message that has a grpc.HookOptions annotation.
//
// The walk visits every message field (including nested messages, list elements, and map values).
// For each message encountered, it checks for the grpc.hook extension. If found, and the
// hook's on_request/on_response flag matches the isRequest parameter, the registered handlers
// for that hook key are invoked.
func invokeHooks(ctx context.Context, callMetadata *CallMetadata, message proto.Message, isRequest bool) error {
	// hookErr captures the first error from a hook handler, since protorange.Break
	// only stops iteration but doesn't propagate the error.
	var hookErr error
	rangeErr := protorange.Range(message.ProtoReflect(), func(values protopath.Values) error {
		last := values.Index(-1)
		step := last.Step

		// Filter to only visit proto message nodes in the tree.
		// We need to handle four cases:
		// 1. RootStep: the top-level message itself.
		// 2. FieldAccessStep: a singular message field (skip lists/maps as their elements are visited separately).
		// 3. ListIndexStep: an element within a repeated message field.
		// 4. MapIndexStep: a value within a map field where the value type is a message.
		switch step.Kind() {
		case protopath.RootStep:
			// Always visit the root message.
		case protopath.FieldAccessStep:
			fieldDescriptor := step.FieldDescriptor()
			// Skip list and map fields; their individual elements are visited via ListIndexStep/MapIndexStep.
			if fieldDescriptor.IsList() || fieldDescriptor.IsMap() {
				return nil
			}
			// Skip non-message fields (scalars, enums, etc.).
			if fieldDescriptor.Kind() != protoreflect.MessageKind && fieldDescriptor.Kind() != protoreflect.GroupKind {
				return nil
			}
		case protopath.ListIndexStep:
			// Only process list elements that are messages.
			prev := values.Index(-2).Step
			if prev.Kind() != protopath.FieldAccessStep {
				return nil
			}
			if prev.FieldDescriptor().Kind() != protoreflect.MessageKind && prev.FieldDescriptor().Kind() != protoreflect.GroupKind {
				return nil
			}
		case protopath.MapIndexStep:
			// Only process map values that are messages.
			prev := values.Index(-2).Step
			if prev.Kind() != protopath.FieldAccessStep {
				return nil
			}
			mapValue := prev.FieldDescriptor().MapValue()
			if mapValue.Kind() != protoreflect.MessageKind && mapValue.Kind() != protoreflect.GroupKind {
				return nil
			}
		default:
			// Skip any other step types (e.g., AnyExpandStep).
			return nil
		}

		// Extract the protoreflect.Message from the current node.
		reflectMessage, ok := last.Value.Interface().(protoreflect.Message)
		if !ok {
			return nil
		}

		// Check if this message type has a grpc.hook annotation.
		descriptor := reflectMessage.Descriptor()
		hookOptions, err := pbutil.GetExtension[*grpcpb.HookOptions](descriptor.Options(), grpcpb.E_Hook)
		if err != nil {
			if errors.Is(err, pbutil.ErrExtensionNotFound) {
				// No hook annotation on this message type; skip.
				return nil
			}
			hookErr = fmt.Errorf("getting hook extension for %q: %w", descriptor.FullName(), err)
			return protorange.Terminate
		}

		// Only invoke if the hook direction matches (request vs response).
		shouldInvoke := (isRequest && hookOptions.GetOnRequest()) || (!isRequest && hookOptions.GetOnResponse())
		if !shouldInvoke {
			return nil
		}

		// Invoke all registered handlers for this hook key.
		if err := invokeRegistrations(ctx, callMetadata, hookOptions.GetKey(), reflectMessage.Interface()); err != nil {
			hookErr = err
			return protorange.Terminate
		}
		return nil
	})
	if rangeErr != nil {
		return rangeErr
	}
	return hookErr
}

// invokeRegistrations looks up all handlers registered for the given hook key
// and invokes them in order. If a handler has a matcher, it is checked first;
// handlers whose matcher returns false are skipped.
func invokeRegistrations(ctx context.Context, callMetadata *CallMetadata, hookKey string, message proto.Message) error {
	globalHookRegistry.mu.RLock()
	registrations := globalHookRegistry.hookKeyToRegistrations[hookKey]
	globalHookRegistry.mu.RUnlock()
	for _, registration := range registrations {
		// Skip this handler if its matcher rejects the current RPC.
		if registration.matcher != nil && !registration.matcher(ctx, callMetadata) {
			continue
		}
		if err := registration.handler(ctx, message); err != nil {
			return err
		}
	}
	return nil
}
