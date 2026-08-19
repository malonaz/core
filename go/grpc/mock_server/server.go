// Package mockserver provides a gRPC server that stands in for a third party in tests. It is
// configured through an Expect() recorder and verified with AssertExpectations.
//
// Expectations are declared against a service through the typed recorder protoc-gen-core's mock
// plugin generates for it, which wraps this package's untyped Recorder so that the endpoint is
// named by the compiler rather than by a string.
package mockserver

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/mock"
)

// Server serves canned responses for any proto service. Requests are routed through an
// unknown-service handler rather than generated server stubs, so an expectation names its
// endpoint by fully-qualified method name and the request and response types are resolved from
// the proto registry.
//
// Calls that match no expectation fail with codes.Unimplemented and are reported by
// AssertExpectations.
type Server struct {
	server   *grpc.Server
	listener net.Listener

	mutex           sync.Mutex
	unaryCalls      []*UnaryCall
	requests        []*Request
	unexpectedCalls []string
}

// Request is a single request the server received. Its method name is spelled the way gRPC and
// the generated stubs' <Service>_<Method>_FullMethodName constants spell it, so an assertion on
// it can name the endpoint through one of those constants.
type Request struct {
	FullMethodName string
	Message        proto.Message
}

// NewServer starts a mock gRPC server on the given port. A port of 0 binds an ephemeral one,
// which Address reports. The listener is bound before returning, so a port clash surfaces here
// rather than asynchronously.
func NewServer(host string, port int) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("listening on %s:%d: %w", host, port, err)
	}
	server := &Server{listener: listener}
	server.server = grpc.NewServer(
		grpc.ForceServerCodec(codec{}),
		grpc.UnknownServiceHandler(server.handle),
	)
	// Core's gRPC clients health check the dependencies they are pointed at, so a mock that does
	// not serve health checks fails its consumers before any RPC is made.
	grpc_health_v1.RegisterHealthServer(server.server, healthServer{})
	go server.server.Serve(listener)
	return server, nil
}

// Address returns the host:port the mock is listening on.
func (s *Server) Address() string { return s.listener.Addr().String() }

// Host returns the host clients should be pointed at.
func (s *Server) Host() string {
	host, _, _ := net.SplitHostPort(s.listener.Addr().String())
	return host
}

// Port returns the port clients should be pointed at.
func (s *Server) Port() int { return s.listener.Addr().(*net.TCPAddr).Port }

// Close stops the server and releases the port.
func (s *Server) Close() { s.server.Stop() }

// Expect returns a recorder for declaring expectations.
func (s *Server) Expect() *Recorder { return &Recorder{server: s} }

// Requests returns a copy of every request received so far, including unmatched ones.
func (s *Server) Requests() []*Request {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]*Request(nil), s.requests...)
}

// Reset drops every expectation and recorded request, so each test starts from a clean slate.
func (s *Server) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.unaryCalls = nil
	s.requests = nil
	s.unexpectedCalls = nil
}

// AssertExpectations fails the test if any expectation went unmet, or any call matched none.
func (s *Server) AssertExpectations(t mock.TestingT) bool {
	t.Helper()
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ok := true
	for _, unaryCall := range s.unaryCalls {
		reason := unaryCall.Verify()
		if reason == "" {
			continue
		}
		ok = false
		t.Errorf("mock gRPC: %s: %s", unaryCall.fullMethodName, reason)
	}
	for _, fullMethodName := range s.unexpectedCalls {
		ok = false
		t.Errorf("mock gRPC: unexpected call to %s", fullMethodName)
	}
	return ok
}

func (s *Server) handle(_ any, stream grpc.ServerStream) error {
	fullMethodName, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Errorf(codes.Internal, "no method name on server stream").Err()
	}
	fullMethodName = canonicalFullMethodName(fullMethodName)

	requestFrame := &frame{}
	if err := stream.RecvMsg(requestFrame); err != nil {
		return status.Errorf(codes.Internal, "receiving request for %s: %v", fullMethodName, err).Err()
	}

	request, err := newRequestMessage(fullMethodName)
	if err != nil {
		return status.Errorf(codes.Unimplemented, "%v", err).Err()
	}
	if err := proto.Unmarshal(requestFrame.payload, request); err != nil {
		return status.Errorf(codes.InvalidArgument, "unmarshaling request for %s: %v", fullMethodName, err).Err()
	}

	unaryCall, err := s.match(fullMethodName, request)
	if err != nil {
		return err
	}

	response, err := unaryCall.reply(stream.Context(), fullMethodName, request)
	if err != nil {
		return err
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		return status.Errorf(codes.Internal, "marshaling response for %s: %v", fullMethodName, err).Err()
	}
	if err := stream.SendMsg(&frame{payload: responseBytes}); err != nil {
		return status.Errorf(codes.Internal, "sending response for %s: %v", fullMethodName, err).Err()
	}
	return nil
}

// match finds the first unexhausted expectation for the request, recording the request either
// way so that Requests reflects everything the mock saw.
func (s *Server) match(fullMethodName string, request proto.Message) (*UnaryCall, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.requests = append(s.requests, &Request{FullMethodName: fullMethodName, Message: request})

	for _, unaryCall := range s.unaryCalls {
		if unaryCall.fullMethodName != fullMethodName || unaryCall.Exhausted() {
			continue
		}
		if unaryCall.payload != nil && !proto.Equal(unaryCall.payload, request) {
			continue
		}
		unaryCall.Record()
		return unaryCall, nil
	}

	s.unexpectedCalls = append(s.unexpectedCalls, fullMethodName)
	return nil, status.Errorf(codes.Unimplemented, "no expectation matched %s", fullMethodName).Err()
}

// canonicalFullMethodName spells a method name the way gRPC does, with a leading slash, which is
// also how the generated stubs spell it in their <Service>_<Method>_FullMethodName constants. An
// expectation may be declared either way, and both resolve to this.
func canonicalFullMethodName(fullMethodName string) string {
	if strings.HasPrefix(fullMethodName, "/") {
		return fullMethodName
	}
	return "/" + fullMethodName
}

func methodDescriptor(fullMethodName string) (protoreflect.MethodDescriptor, error) {
	serviceName, methodName, ok := strings.Cut(strings.TrimPrefix(fullMethodName, "/"), "/")
	if !ok {
		return nil, fmt.Errorf("method name %q is not of the form /package.Service/Method", fullMethodName)
	}
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, fmt.Errorf("finding service %q: %w", serviceName, err)
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is a %T, not a service", serviceName, descriptor)
	}
	methodDescriptor := serviceDescriptor.Methods().ByName(protoreflect.Name(methodName))
	if methodDescriptor == nil {
		return nil, fmt.Errorf("service %q has no method %q", serviceName, methodName)
	}
	return methodDescriptor, nil
}

func newMessage(name protoreflect.FullName) (proto.Message, error) {
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, fmt.Errorf("finding message %q: %w", name, err)
	}
	return messageType.New().Interface(), nil
}

func newRequestMessage(fullMethodName string) (proto.Message, error) {
	methodDescriptor, err := methodDescriptor(fullMethodName)
	if err != nil {
		return nil, err
	}
	return newMessage(methodDescriptor.Input().FullName())
}

func newResponseMessage(fullMethodName string) (proto.Message, error) {
	methodDescriptor, err := methodDescriptor(fullMethodName)
	if err != nil {
		return nil, err
	}
	return newMessage(methodDescriptor.Output().FullName())
}
