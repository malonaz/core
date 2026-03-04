package middleware

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// HookHandler is a typed function that processes a proto message when a hook is triggered.
// The handler receives the context and the specific proto message type it was registered for.
type HookHandler[T proto.Message] func(ctx context.Context, message T) error

// HookMatcher determines whether a hook should be invoked for a given RPC.
// It receives the context and call metadata containing the service name, method name, and RPC type.
// Returning true means the hook will be invoked; false means it will be skipped.
type HookMatcher func(ctx context.Context, callMetadata *CallMetadata) bool

// MatchMethods returns a HookMatcher that matches if the RPC method is in the given set.
func MatchMethods(methods ...string) HookMatcher {
	methodSet := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		methodSet[m] = struct{}{}
	}
	return func(_ context.Context, callMetadata *CallMetadata) bool {
		_, ok := methodSet[callMetadata.Method]
		return ok
	}
}

// MatchServices returns a HookMatcher that matches if the RPC service is in the given set.
func MatchServices(services ...string) HookMatcher {
	serviceSet := make(map[string]struct{}, len(services))
	for _, s := range services {
		serviceSet[s] = struct{}{}
	}
	return func(_ context.Context, callMetadata *CallMetadata) bool {
		_, ok := serviceSet[callMetadata.Service]
		return ok
	}
}

// MatchFullMethods returns a HookMatcher that matches if the full method (service.method) is in the given set.
func MatchFullMethods(fullMethods ...string) HookMatcher {
	fullMethodSet := make(map[string]struct{}, len(fullMethods))
	for _, fm := range fullMethods {
		fullMethodSet[fm] = struct{}{}
	}
	return func(_ context.Context, callMetadata *CallMetadata) bool {
		_, ok := fullMethodSet[callMetadata.FullMethod()]
		return ok
	}
}

// HookOption configures optional behavior for a hook registration.
type HookOption func(*hookRegistration)

// WithHookMatchers sets matcher functions on a hook registration.
// Can be called multiple times to add additional matchers.
// All matchers must return true for the hook to be invoked (AND semantics).
// If no matchers are set, the hook is invoked for all RPCs that contain the registered message type.
func WithHookMatchers(matchers ...HookMatcher) HookOption {
	return func(r *hookRegistration) {
		r.matchers = append(r.matchers, matchers...)
	}
}

// WithHookOnRequest configures the hook to be invoked when the registered message type
// appears anywhere in the request message tree of an RPC.
func WithHookOnRequest() HookOption {
	return func(r *hookRegistration) {
		r.onRequest = true
	}
}

// WithHookOnResponse configures the hook to be invoked when the registered message type
// appears anywhere in the response message tree of an RPC.
func WithHookOnResponse() HookOption {
	return func(r *hookRegistration) {
		r.onResponse = true
	}
}

// hookRegistration stores a single hook handler along with its optional matcher
// and direction flags. The handler accepts a generic proto.Message since the
// type-specific wrapper is created at registration time via RegisterHookHandler.
type hookRegistration struct {
	handler    func(context.Context, proto.Message) error
	matchers   []HookMatcher
	onRequest  bool
	onResponse bool
}

// hookRegistry is a thread-safe registry mapping message full names to their registered handlers.
// Message full names are derived from the proto descriptor of the message type at registration time.
type hookRegistry struct {
	mu                      sync.RWMutex
	fullNameToRegistrations map[string][]*hookRegistration
}

// hasMatchingRegistrations checks whether any registered hooks match the given call metadata
// for each direction (request and response). It evaluates all matchers (AND semantics) for
// each registration and returns two booleans indicating whether at least one registration
// matched for request and response respectively. This avoids allocating new maps per RPC.
func (r *hookRegistry) hasMatchingRegistrations(ctx context.Context, callMetadata *CallMetadata) (bool, bool) {
	var hasRequest, hasResponse bool
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, registrations := range r.fullNameToRegistrations {
		for _, registration := range registrations {
			matched := true
			for _, matcher := range registration.matchers {
				if !matcher(ctx, callMetadata) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if registration.onRequest {
				hasRequest = true
			}
			if registration.onResponse {
				hasResponse = true
			}
			// Short-circuit once both directions are matched.
			if hasRequest && hasResponse {
				return hasRequest, hasResponse
			}
		}
	}
	return hasRequest, hasResponse
}

// globalHookRegistry is the singleton registry used by all hook interceptors.
// Handlers are registered at init time via RegisterHookHandler and looked up at request time.
var globalHookRegistry = &hookRegistry{
	fullNameToRegistrations: make(map[string][]*hookRegistration),
}

// RegisterHookHandler registers a typed hook handler for the proto message type T.
// The lookup key is derived from T's proto descriptor full name. At least one of
// WithOnRequest or WithOnResponse must be provided to specify when the hook is invoked.
// The generic type parameter T ensures type safety: the handler will only be called
// with messages of the correct type. This function is typically called during init().
func RegisterHookHandler[T proto.Message](handler HookHandler[T], opts ...HookOption) error {
	var zero T
	descriptor := zero.ProtoReflect().Descriptor()
	fullName := string(descriptor.FullName())
	registration := &hookRegistration{
		// Wrap the typed handler in a generic proto.Message handler.
		// The type assertion is safe because the hook system only invokes handlers
		// with messages whose proto type matches the registered message.
		handler: func(ctx context.Context, message proto.Message) error {
			return handler(ctx, message.(T))
		},
	}
	for _, opt := range opts {
		opt(registration)
	}
	if !registration.onRequest && !registration.onResponse {
		registration.onRequest = true
		registration.onResponse = true
	}
	globalHookRegistry.mu.Lock()
	defer globalHookRegistry.mu.Unlock()
	globalHookRegistry.fullNameToRegistrations[fullName] = append(globalHookRegistry.fullNameToRegistrations[fullName], registration)
	return nil
}

// UnaryServerHook returns a unary server interceptor that invokes hooks on request and response messages.
// For requests, hooks registered with WithOnRequest are invoked before the handler.
// For responses, hooks registered with WithOnResponse are invoked after the handler succeeds.
// Matchers are evaluated once per RPC; if no registrations match for a direction, the
// message tree walk is skipped entirely for that direction.
func UnaryServerHook() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Build call metadata once for this RPC, used by all hook matchers.
		callMetadata := newServerCallMetadata(info.FullMethod, nil, req)

		// Pre-check whether any registrations match to avoid unnecessary tree walks.
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(ctx, callMetadata)

		// Invoke request hooks before calling the handler.
		if hasRequest {
			if message, ok := req.(proto.Message); ok {
				if err := invokeHooks(ctx, callMetadata, message, true); err != nil {
					return nil, err
				}
			}
		}

		response, err := handler(ctx, req)
		if err != nil {
			return response, err
		}

		// Invoke response hooks after the handler succeeds.
		if hasResponse {
			if message, ok := response.(proto.Message); ok {
				if err := invokeHooks(ctx, callMetadata, message, false); err != nil {
					return nil, err
				}
			}
		}
		return response, err
	}
}

// StreamServerHook returns a stream server interceptor that invokes hooks on streamed messages.
// Request hooks are invoked on received messages; response hooks are invoked on sent messages.
// If no registrations match the stream's call metadata, the stream is passed through unwrapped.
func StreamServerHook() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Build call metadata for this stream RPC.
		callMetadata := newServerCallMetadata(info.FullMethod, info, nil)

		// Pre-check whether any registrations match; skip wrapping if nothing matches.
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(stream.Context(), callMetadata)
		if !hasRequest && !hasResponse {
			return handler(srv, stream)
		}
		return handler(srv, &hookServerStream{
			ServerStream: stream,
			callMetadata: callMetadata,
			hasRequest:   hasRequest,
			hasResponse:  hasResponse,
		})
	}
}

// hookServerStream wraps a grpc.ServerStream to intercept sent and received messages
// and invoke the appropriate hooks on them. The hasRequest and hasResponse flags
// are pre-computed from matcher evaluation to avoid redundant checks per message.
type hookServerStream struct {
	grpc.ServerStream
	callMetadata *CallMetadata
	hasRequest   bool
	hasResponse  bool
}

// SendMsg intercepts outgoing stream messages and invokes response hooks before sending.
func (s *hookServerStream) SendMsg(m any) error {
	if s.hasResponse {
		if message, ok := m.(proto.Message); ok {
			if err := invokeHooks(s.Context(), s.callMetadata, message, false); err != nil {
				return err
			}
		}
	}
	return s.ServerStream.SendMsg(m)
}

// RecvMsg intercepts incoming stream messages and invokes request hooks after receiving.
func (s *hookServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if s.hasRequest {
		if message, ok := m.(proto.Message); ok {
			if err := invokeHooks(s.Context(), s.callMetadata, message, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// invokeHooks walks the proto message tree using protorange and invokes registered hooks
// for any message whose full name has registrations in the global hook registry.
//
// The walk visits every message field (including nested messages, list elements, and map values).
// For each message encountered, it checks the registry for handlers registered against that
// message's full name. Handlers are filtered by direction (request vs response) and matchers
// against the call metadata.
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
			// Skip unset message fields to avoid infinite recursion into default values.
			parent := values.Index(-2)
			parentMessage, ok := parent.Value.Interface().(protoreflect.Message)
			if !ok || !parentMessage.Has(fieldDescriptor) {
				return protorange.Break
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
		if !ok || !reflectMessage.IsValid() {
			return protorange.Break
		}

		// Look up registrations by message full name and invoke matching handlers.
		fullName := string(reflectMessage.Descriptor().FullName())
		if err := invokeRegistrations(ctx, callMetadata, fullName, isRequest, reflectMessage.Interface()); err != nil {
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

// invokeRegistrations looks up all handlers registered for the given message full name
// and invokes them in order, filtered by direction and matchers. Handlers whose direction
// doesn't match or whose matcher rejects the current RPC are skipped.
func invokeRegistrations(ctx context.Context, callMetadata *CallMetadata, fullName string, isRequest bool, message proto.Message) error {
	globalHookRegistry.mu.RLock()
	registrations := globalHookRegistry.fullNameToRegistrations[fullName]
	globalHookRegistry.mu.RUnlock()
	for _, registration := range registrations {
		// Skip this handler if the direction doesn't match.
		if isRequest && !registration.onRequest {
			continue
		}
		if !isRequest && !registration.onResponse {
			continue
		}
		// Skip this handler if its matcher rejects the current RPC.
		matched := true
		for _, matcher := range registration.matchers {
			if !matcher(ctx, callMetadata) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if err := registration.handler(ctx, message); err != nil {
			return err
		}
	}
	return nil
}
