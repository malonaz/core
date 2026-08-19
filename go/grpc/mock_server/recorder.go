package mockserver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/mock"
)

// Recorder declares expectations on a Server.
type Recorder struct {
	server *Server
}

// UnaryCall is an expectation on a unary endpoint.
type UnaryCall struct {
	mock.Cardinality
	fullMethodName string
	payload        proto.Message
	response       proto.Message
	handler        handlerFunc
	err            error
}

// Unary registers an expectation for a unary endpoint, named by its fully-qualified method
// name, with or without a leading slash: "package.Service/Method". Expectations are matched in
// declaration order, and one declared with Times stops matching once it is exhausted, so
// several expectations on the same endpoint reply in the order they were declared.
//
// Prefer the typed recorder generated for the service by protoc-gen-core's mock plugin, which
// names the endpoint and types the request and response for you. This is the escape hatch for
// a service whose generated mock is not on hand.
func (r *Recorder) Unary(fullMethodName string) *UnaryCall {
	unaryCall := &UnaryCall{
		Cardinality:    mock.NewCardinality(),
		fullMethodName: normalizeFullMethodName(fullMethodName),
	}
	r.server.mutex.Lock()
	defer r.server.mutex.Unlock()
	r.server.unaryCalls = append(r.server.unaryCalls, unaryCall)
	return unaryCall
}

// WithPayload restricts the expectation to requests equal to the given message. Without it,
// any request to the endpoint matches.
func (c *UnaryCall) WithPayload(payload proto.Message) *UnaryCall {
	c.payload = payload
	return c
}

// Return sets the message to reply with. Without it, the endpoint's zero-value response
// message is returned.
func (c *UnaryCall) Return(response proto.Message) *UnaryCall {
	c.response = response
	return c
}

// ReturnError sets the error to fail with. Pass a status error to control the code the caller
// sees; anything else surfaces as codes.Unknown, as it would from a real server.
func (c *UnaryCall) ReturnError(err error) *UnaryCall {
	c.err = err
	return c
}

// handleAny sets the function that computes the reply. Handle is the only way to reach it, so
// that every handler is checked against the types it is declared with.
func (c *UnaryCall) handleAny(handler handlerFunc) *UnaryCall {
	c.handler = handler
	return c
}

// Handle computes the reply from the request, for an endpoint whose response depends on what it
// was called with, with a function typed to that endpoint:
//
//	mockserver.Handle(server.EXPECT().Unary(pb.MockService_Echo_FullMethodName),
//		func(ctx context.Context, request *pb.EchoRequest) (*pb.EchoResponse, error) { ... },
//	)
//
// It takes precedence over Return and ReturnError. It is a function rather than a method on
// UnaryCall because Go has no generic methods. The generated typed recorders wrap it, so a test
// declaring its expectations through those never names the request and response types itself; a
// handler that means to work in terms of proto.Message instantiates it at that type instead.
func Handle[Request, Response proto.Message](
	unaryCall *UnaryCall, handler func(context.Context, Request) (Response, error),
) *UnaryCall {
	return unaryCall.handleAny(func(ctx context.Context, request proto.Message) (proto.Message, error) {
		// The request was decoded from the registry, so it is already the handler's own type
		// unless the expectation names an endpoint the handler was not written for. Generated
		// recorders pair the two, so this only fires for a hand-written expectation.
		typedRequest, ok := request.(Request)
		if !ok {
			return nil, status.Errorf(
				codes.Internal, "handler takes %T, but the endpoint's request is %T", typedRequest, request,
			).Err()
		}
		return handler(ctx, typedRequest)
	})
}

// Times requires the expectation to be matched exactly count times.
func (c *UnaryCall) Times(count int) *UnaryCall {
	c.Cardinality.Times(count)
	return c
}

// AnyTimes lets the expectation match any number of times, including none. Use it for
// endpoints a dependency calls on its own schedule, which no test asserts on.
func (c *UnaryCall) AnyTimes() *UnaryCall {
	c.Cardinality.AnyTimes()
	return c
}

// reply produces the message the matched expectation replies with, in precedence order: a
// handler computes it, then a canned error or response, and failing all of those the endpoint's
// zero-value response message.
func (c *UnaryCall) reply(ctx context.Context, fullMethodName string, request proto.Message) (proto.Message, error) {
	if c.handler != nil {
		response, err := c.handler(ctx, request)
		if err != nil {
			return nil, err
		}
		// A typed nil is not a message the server can reply with, and reads at the client as an
		// empty response rather than as the handler bug it is.
		if response == nil || !response.ProtoReflect().IsValid() {
			return nil, status.Errorf(
				codes.Internal, "%s: handler returned no response and no error", fullMethodName,
			).Err()
		}
		return response, nil
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.response != nil {
		return c.response, nil
	}
	response, err := newResponseMessage(fullMethodName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return response, nil
}

// handlerFunc calls a function with the decoded request.
type handlerFunc func(ctx context.Context, request proto.Message) (proto.Message, error)
