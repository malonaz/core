package middleware

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func resetGlobalHookRegistry() {
	globalHookRegistry.mu.Lock()
	defer globalHookRegistry.mu.Unlock()
	globalHookRegistry.fullNameToRegistrations = make(map[string][]*hookRegistration)
}

func newTestCallMetadata(service, method string) *CallMetadata {
	fullMethod := fmt.Sprintf("/%s/%s", service, method)
	return newServerCallMetadata(fullMethod, nil, nil)
}

func TestRegisterHookHandler(t *testing.T) {
	t.Run("defaults to both request and response", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		})
		require.NoError(t, err)

		globalHookRegistry.mu.RLock()
		registrations := globalHookRegistry.fullNameToRegistrations["google.protobuf.Timestamp"]
		globalHookRegistry.mu.RUnlock()
		require.Len(t, registrations, 1)
		require.True(t, registrations[0].onRequest)
		require.True(t, registrations[0].onResponse)
	})

	t.Run("respects WithHookOnRequest only", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		globalHookRegistry.mu.RLock()
		registrations := globalHookRegistry.fullNameToRegistrations["google.protobuf.Timestamp"]
		globalHookRegistry.mu.RUnlock()
		require.Len(t, registrations, 1)
		require.True(t, registrations[0].onRequest)
		require.False(t, registrations[0].onResponse)
	})

	t.Run("respects WithHookOnResponse only", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)

		globalHookRegistry.mu.RLock()
		registrations := globalHookRegistry.fullNameToRegistrations["google.protobuf.Timestamp"]
		globalHookRegistry.mu.RUnlock()
		require.Len(t, registrations, 1)
		require.False(t, registrations[0].onRequest)
		require.True(t, registrations[0].onResponse)
	})

	t.Run("multiple registrations for same type", func(t *testing.T) {
		resetGlobalHookRegistry()
		for i := 0; i < 3; i++ {
			err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
				return nil
			})
			require.NoError(t, err)
		}

		globalHookRegistry.mu.RLock()
		registrations := globalHookRegistry.fullNameToRegistrations["google.protobuf.Timestamp"]
		globalHookRegistry.mu.RUnlock()
		require.Len(t, registrations, 3)
	})
}

func TestHasMatchingRegistrations(t *testing.T) {
	t.Run("no registrations", func(t *testing.T) {
		resetGlobalHookRegistry()
		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.False(t, hasRequest)
		require.False(t, hasResponse)
	})

	t.Run("registration with no matchers matches everything", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		})
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("AnyService", "AnyMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.True(t, hasRequest)
		require.True(t, hasResponse)
	})

	t.Run("matcher rejects RPC", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookMatchers(MatchMethods("AllowedMethod")))
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "OtherMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.False(t, hasRequest)
		require.False(t, hasResponse)
	})

	t.Run("matcher accepts RPC", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookMatchers(MatchMethods("AllowedMethod")))
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "AllowedMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.True(t, hasRequest)
		require.True(t, hasResponse)
	})

	t.Run("request only registration", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.True(t, hasRequest)
		require.False(t, hasResponse)
	})

	t.Run("response only registration", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.False(t, hasRequest)
		require.True(t, hasResponse)
	})

	t.Run("multiple matchers AND semantics", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return nil
		}, WithHookMatchers(MatchMethods("TestMethod"), MatchServices("TestService")))
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		hasRequest, hasResponse := globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.True(t, hasRequest)
		require.True(t, hasResponse)

		callMetadata = newTestCallMetadata("OtherService", "TestMethod")
		hasRequest, hasResponse = globalHookRegistry.hasMatchingRegistrations(context.Background(), callMetadata)
		require.False(t, hasRequest)
		require.False(t, hasResponse)
	})
}

func TestMatchMethods(t *testing.T) {
	matcher := MatchMethods("Foo", "Bar")
	require.True(t, matcher(context.Background(), newTestCallMetadata("Svc", "Foo")))
	require.True(t, matcher(context.Background(), newTestCallMetadata("Svc", "Bar")))
	require.False(t, matcher(context.Background(), newTestCallMetadata("Svc", "Baz")))
}

func TestMatchServices(t *testing.T) {
	matcher := MatchServices("ServiceA", "ServiceB")
	require.True(t, matcher(context.Background(), newTestCallMetadata("ServiceA", "M")))
	require.True(t, matcher(context.Background(), newTestCallMetadata("ServiceB", "M")))
	require.False(t, matcher(context.Background(), newTestCallMetadata("ServiceC", "M")))
}

func TestMatchFullMethods(t *testing.T) {
	matcher := MatchFullMethods("ServiceA.Foo")
	require.True(t, matcher(context.Background(), newTestCallMetadata("ServiceA", "Foo")))
	require.False(t, matcher(context.Background(), newTestCallMetadata("ServiceA", "Bar")))
	require.False(t, matcher(context.Background(), newTestCallMetadata("ServiceB", "Foo")))
}

func TestInvokeHooks(t *testing.T) {
	t.Run("invokes handler on root message", func(t *testing.T) {
		resetGlobalHookRegistry()
		var called bool
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			called = true
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("skips handler when direction is wrong", func(t *testing.T) {
		resetGlobalHookRegistry()
		var called bool
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			called = true
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err = invokeHooks(context.Background(), callMetadata, message, false)
		require.NoError(t, err)
		require.False(t, called)
	})

	t.Run("does not invoke handler for unrelated nested message", func(t *testing.T) {
		resetGlobalHookRegistry()
		var captured *durationpb.Duration
		err := RegisterHookHandler(func(_ context.Context, duration *durationpb.Duration) error {
			captured = duration
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.Nil(t, captured)
	})

	t.Run("propagates handler error", func(t *testing.T) {
		resetGlobalHookRegistry()
		expectedErr := fmt.Errorf("hook failed")
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			return expectedErr
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("skips handler when matcher rejects", func(t *testing.T) {
		resetGlobalHookRegistry()
		var called bool
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			called = true
			return nil
		}, WithHookMatchers(MatchMethods("OtherMethod")))
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.False(t, called)
	})

	t.Run("invokes multiple handlers in order", func(t *testing.T) {
		resetGlobalHookRegistry()
		var order []int
		for i := 0; i < 3; i++ {
			i := i
			err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
				order = append(order, i)
				return nil
			}, WithHookOnRequest())
			require.NoError(t, err)
		}

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err := invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, order)
	})

	t.Run("stops on first handler error", func(t *testing.T) {
		resetGlobalHookRegistry()
		var callCount int
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			callCount++
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)
		err = RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			callCount++
			return fmt.Errorf("fail")
		}, WithHookOnRequest())
		require.NoError(t, err)
		err = RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			callCount++
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &timestamppb.Timestamp{Seconds: 123}
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.Error(t, err)
		require.Equal(t, 2, callCount)
	})

	t.Run("walks nested struct messages", func(t *testing.T) {
		resetGlobalHookRegistry()
		var valueCount int
		err := RegisterHookHandler(func(_ context.Context, _ *structpb.Value) error {
			valueCount++
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		message, err := structpb.NewStruct(map[string]any{
			"key1": "val1",
			"key2": "val2",
		})
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.Equal(t, 2, valueCount)
	})

	t.Run("walks list elements", func(t *testing.T) {
		resetGlobalHookRegistry()
		var valueCount int
		err := RegisterHookHandler(func(_ context.Context, _ *structpb.Value) error {
			valueCount++
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		message, err := structpb.NewList([]any{"a", "b", "c"})
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		err = invokeHooks(context.Background(), callMetadata, message, true)
		require.NoError(t, err)
		require.Equal(t, 3, valueCount)
	})
}

func TestUnaryServerHook(t *testing.T) {
	t.Run("invokes request hook before handler", func(t *testing.T) {
		resetGlobalHookRegistry()
		var hookOrder []string
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			hookOrder = append(hookOrder, "hook")
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			hookOrder = append(hookOrder, "handler")
			return &wrapperspb.StringValue{Value: "response"}, nil
		}

		response, err := interceptor(context.Background(), req, info, handler)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, []string{"hook", "handler"}, hookOrder)
	})

	t.Run("invokes response hook after handler", func(t *testing.T) {
		resetGlobalHookRegistry()
		var hookOrder []string
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			hookOrder = append(hookOrder, "response-hook")
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)

		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			hookOrder = append(hookOrder, "handler")
			return &wrapperspb.StringValue{Value: "response"}, nil
		}

		response, err := interceptor(context.Background(), req, info, handler)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, []string{"handler", "response-hook"}, hookOrder)
	})

	t.Run("request hook error prevents handler from running", func(t *testing.T) {
		resetGlobalHookRegistry()
		var handlerCalled bool
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			return fmt.Errorf("request hook failed")
		}, WithHookOnRequest())
		require.NoError(t, err)

		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			handlerCalled = true
			return &wrapperspb.StringValue{Value: "response"}, nil
		}

		_, err = interceptor(context.Background(), req, info, handler)
		require.Error(t, err)
		require.False(t, handlerCalled)
	})

	t.Run("handler error prevents response hooks from running", func(t *testing.T) {
		resetGlobalHookRegistry()
		var responseHookCalled bool
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			responseHookCalled = true
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)

		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			return nil, fmt.Errorf("handler failed")
		}

		_, err = interceptor(context.Background(), req, info, handler)
		require.Error(t, err)
		require.False(t, responseHookCalled)
	})

	t.Run("skips hooks when no registrations match", func(t *testing.T) {
		resetGlobalHookRegistry()
		var handlerCalled bool
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			return fmt.Errorf("should not be called")
		}, WithHookMatchers(MatchMethods("OtherMethod")))
		require.NoError(t, err)

		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			handlerCalled = true
			return &wrapperspb.StringValue{Value: "response"}, nil
		}

		response, err := interceptor(context.Background(), req, info, handler)
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, handlerCalled)
	})

	t.Run("no registrations at all passes through", func(t *testing.T) {
		resetGlobalHookRegistry()
		interceptor := UnaryServerHook()
		req := &wrapperspb.StringValue{Value: "test"}
		info := &grpc.UnaryServerInfo{FullMethod: "/test.TestService/TestMethod"}
		handler := func(_ context.Context, _ any) (any, error) {
			return &wrapperspb.StringValue{Value: "ok"}, nil
		}

		response, err := interceptor(context.Background(), req, info, handler)
		require.NoError(t, err)
		require.Equal(t, "ok", response.(*wrapperspb.StringValue).GetValue())
	})

	t.Run("hooks on different types both fire", func(t *testing.T) {
		resetGlobalHookRegistry()
		var timestampCalled, durationCalled bool
		err := RegisterHookHandler(func(_ context.Context, _ *timestamppb.Timestamp) error {
			timestampCalled = true
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)
		err = RegisterHookHandler(func(_ context.Context, _ *durationpb.Duration) error {
			durationCalled = true
			return nil
		}, WithHookOnResponse())
		require.NoError(t, err)

		callMetadata := newTestCallMetadata("TestService", "TestMethod")
		message := &structpb.Value{}
		err = invokeHooks(context.Background(), callMetadata, message, false)
		require.NoError(t, err)
		require.False(t, timestampCalled)
		require.False(t, durationCalled)
	})
}

func TestStreamServerHook(t *testing.T) {
	t.Run("passes through when no registrations match", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			return fmt.Errorf("should not be called")
		}, WithHookMatchers(MatchMethods("OtherMethod")))
		require.NoError(t, err)

		interceptor := StreamServerHook()
		stream := &fakeServerStream{ctx: context.Background()}
		info := &grpc.StreamServerInfo{FullMethod: "/test.TestService/TestMethod"}
		var handlerCalled bool
		handlerFunc := func(_ any, _ grpc.ServerStream) error {
			handlerCalled = true
			return nil
		}

		err = interceptor(nil, stream, info, handlerFunc)
		require.NoError(t, err)
		require.True(t, handlerCalled)
	})

	t.Run("wraps stream when registrations match", func(t *testing.T) {
		resetGlobalHookRegistry()
		err := RegisterHookHandler(func(_ context.Context, _ *wrapperspb.StringValue) error {
			return nil
		}, WithHookOnRequest())
		require.NoError(t, err)

		interceptor := StreamServerHook()
		stream := &fakeServerStream{ctx: context.Background()}
		info := &grpc.StreamServerInfo{FullMethod: "/test.TestService/TestMethod"}
		var receivedStream grpc.ServerStream
		handlerFunc := func(_ any, s grpc.ServerStream) error {
			receivedStream = s
			return nil
		}

		err = interceptor(nil, stream, info, handlerFunc)
		require.NoError(t, err)
		_, isWrapped := receivedStream.(*hookServerStream)
		require.True(t, isWrapped)
	})
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context {
	return f.ctx
}
