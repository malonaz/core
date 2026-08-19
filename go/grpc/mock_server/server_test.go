package mockserver_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	mockserver "github.com/malonaz/core/go/grpc/mock_server"
	"github.com/malonaz/core/go/grpc/status"

	testmock "github.com/malonaz/core/gengo/test/mock"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
)

// The untyped recorder is given a method name rather than deriving one, but the name itself
// still comes from the gRPC stub rather than being spelled out here.
const echoMethodName = mockpb.MockService_Echo_FullMethodName

// recorder collects the failures a mock reports, standing in for *testing.T so that the
// unhappy paths can be asserted on rather than failing the test that exercises them.
type recorder struct {
	failures []string
}

func (r *recorder) Errorf(format string, arguments ...any) {
	r.failures = append(r.failures, format)
}

func (r *recorder) Helper() {}

func newTestServer(t *testing.T) (*mockserver.Server, mockpb.MockServiceClient) {
	t.Helper()
	server, err := mockserver.NewServer("localhost", 0)
	require.NoError(t, err)
	t.Cleanup(server.Close)

	connection, err := grpc.NewClient(server.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close() })
	return server, mockpb.NewMockServiceClient(connection)
}

func TestReturn(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	echoResponse := &mockpb.EchoResponse{Message: "next"}
	server.Expect().Unary(echoMethodName).Return(echoResponse)

	echoRequest := &mockpb.EchoRequest{Count: 10}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "next", response.Message)

	require.True(t, server.AssertExpectations(t))
	requests := server.Requests()
	require.Len(t, requests, 1)
	require.Equal(t, echoMethodName, requests[0].FullMethodName)
	require.Equal(t, int32(10), requests[0].Message.(*mockpb.EchoRequest).Count)
}

// A leading slash is how gRPC and the generated stub's constant spell a full method name, but an
// expectation declared without one resolves to the same endpoint.
func TestLeadingSlash(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().Unary(strings.TrimPrefix(echoMethodName, "/"))

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.True(t, server.AssertExpectations(t))
}

// Without Return, the endpoint's zero-value response is served.
func TestDefaultResponse(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().Unary(echoMethodName)

	echoRequest := &mockpb.EchoRequest{}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Empty(t, response.Message)
}

func TestWithPayload(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().
		Unary(echoMethodName).
		WithPayload(&mockpb.EchoRequest{Count: 1}).
		Return(&mockpb.EchoResponse{Message: "matched"})

	matching := &mockpb.EchoRequest{Count: 1}
	response, err := mockServiceClient.Echo(context.Background(), matching)
	require.NoError(t, err)
	require.Equal(t, "matched", response.Message)

	// A request the expectation does not describe falls through to no expectation at all.
	nonMatching := &mockpb.EchoRequest{Count: 2}
	_, err = mockServiceClient.Echo(context.Background(), nonMatching)
	require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
}

// Expectations are consumed in order, so the same endpoint can reply differently per call.
func TestTimesOrdersResponses(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().
		Unary(echoMethodName).
		Return(&mockpb.EchoResponse{Message: "first"}).
		Times(1)
	server.Expect().
		Unary(echoMethodName).
		Return(&mockpb.EchoResponse{Message: "second"}).
		Times(1)

	echoRequest := &mockpb.EchoRequest{}
	first, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "first", first.Message)

	second, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "second", second.Message)

	require.True(t, server.AssertExpectations(t))
}

// Status errors reach the caller with their code intact.
func TestReturnError(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().
		Unary(echoMethodName).
		ReturnError(status.Errorf(codes.ResourceExhausted, "slow down").Err())

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.Equal(t, codes.ResourceExhausted, grpcstatus.Code(err))
	require.True(t, server.AssertExpectations(t))
}

// Handle computes the reply from the request, for an endpoint whose response depends on what it
// was called with.
func TestHandle(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockserver.Handle(
		server.Expect().Unary(echoMethodName),
		func(_ context.Context, echoRequest *mockpb.EchoRequest) (*mockpb.EchoResponse, error) {
			return &mockpb.EchoResponse{Message: echoRequest.Message, Count: echoRequest.Count * 2}, nil
		},
	)

	echoRequest := &mockpb.EchoRequest{Message: "hello", Count: 21}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "hello", response.Message)
	require.Equal(t, int32(42), response.Count)
	require.True(t, server.AssertExpectations(t))
}

// An error from the handler reaches the caller with its code intact, as ReturnError's does.
func TestHandleReturnsError(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockserver.Handle(
		server.Expect().Unary(echoMethodName),
		func(context.Context, *mockpb.EchoRequest) (*mockpb.EchoResponse, error) {
			return nil, status.Errorf(codes.FailedPrecondition, "not today").Err()
		},
	)

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
	require.True(t, server.AssertExpectations(t))
}

// A handler that returns neither a response nor an error is a bug in the test, and reads at the
// caller as an empty response unless the mock says so.
func TestHandleRejectsEmptyReply(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockserver.Handle(
		server.Expect().Unary(echoMethodName),
		func(context.Context, *mockpb.EchoRequest) (*mockpb.EchoResponse, error) { return nil, nil },
	)

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.Equal(t, codes.Internal, grpcstatus.Code(err))
}

// The generated recorders pair an endpoint with the types it was declared with, but a
// hand-written expectation can still point a handler at the wrong endpoint, which fails the call
// rather than panicking mid-RPC.
func TestHandleRejectsWrongRequestType(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockserver.Handle(
		server.Expect().Unary(echoMethodName),
		func(context.Context, *mockpb.EchoResponse) (*mockpb.EchoResponse, error) {
			return &mockpb.EchoResponse{}, nil
		},
	)

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.Equal(t, codes.Internal, grpcstatus.Code(err))
}

func TestAssertExpectationsReportsUnmetExpectation(t *testing.T) {
	server, _ := newTestServer(t)
	server.Expect().Unary(echoMethodName)

	recorder := &recorder{}
	require.False(t, server.AssertExpectations(recorder))
	require.Len(t, recorder.failures, 1)
}

func TestAssertExpectationsReportsUnexpectedCall(t *testing.T) {
	server, mockServiceClient := newTestServer(t)

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))

	recorder := &recorder{}
	require.False(t, server.AssertExpectations(recorder))
	require.Len(t, recorder.failures, 1)
}

func TestAnyTimesIsOptional(t *testing.T) {
	server, _ := newTestServer(t)
	server.Expect().Unary(echoMethodName).AnyTimes()
	require.True(t, server.AssertExpectations(t))
}

func TestReset(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	server.Expect().Unary(echoMethodName)

	echoRequest := &mockpb.EchoRequest{}
	_, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)

	server.Reset()
	require.Empty(t, server.Requests())
	require.True(t, server.AssertExpectations(t))
}

// Core's gRPC clients health check the dependencies they are pointed at, so the mock has to
// answer health probes as well as the endpoints it was given expectations for.
func TestServesHealthChecks(t *testing.T) {
	server, _ := newTestServer(t)
	connection, err := grpc.NewClient(server.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer connection.Close()

	healthCheckRequest := &grpc_health_v1.HealthCheckRequest{Service: mockpb.MockService_ServiceDesc.ServiceName}
	healthCheckResponse, err := grpc_health_v1.NewHealthClient(connection).Check(context.Background(), healthCheckRequest)
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, healthCheckResponse.Status)

	// Health traffic is not RPC traffic: it must not show up as an unexpected call.
	require.True(t, server.AssertExpectations(t))
}

// The generated recorder names the endpoint from the gRPC stub and types the request and
// response, so a test declaring its expectations through it never spells a method name.
func TestGeneratedRecorder(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockService := testmock.NewMockServiceMock(server)
	mockService.Expect().
		Echo().
		WithPayload(&mockpb.EchoRequest{Count: 1}).
		Return(&mockpb.EchoResponse{Message: "generated"}).
		Times(1)

	echoRequest := &mockpb.EchoRequest{Count: 1}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "generated", response.Message)
	require.True(t, server.AssertExpectations(t))
}

// The generated Handle takes a function typed to the endpoint, so the signature the untyped
// recorder could only check at runtime is checked by the compiler.
func TestGeneratedRecorderHandle(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockService := testmock.NewMockServiceMock(server)
	mockService.Expect().
		Echo().
		Handle(func(_ context.Context, echoRequest *mockpb.EchoRequest) (*mockpb.EchoResponse, error) {
			return &mockpb.EchoResponse{Count: echoRequest.Count * 2}, nil
		})

	echoRequest := &mockpb.EchoRequest{Count: 21}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, int32(42), response.Count)
	require.True(t, server.AssertExpectations(t))
}

// Expectations declared through the generated recorder are the same ones the server verifies,
// so an unmet one is reported the same way.
func TestGeneratedRecorderReportsUnmetExpectation(t *testing.T) {
	server, _ := newTestServer(t)
	testmock.NewMockServiceMock(server).Expect().Echo()

	recorder := &recorder{}
	require.False(t, server.AssertExpectations(recorder))
	require.Len(t, recorder.failures, 1)
}

// A handler that means to work in terms of proto.Message rather than the endpoint's own types
// instantiates Handle at that type, so the untyped path needs no entry point of its own.
func TestHandleWithUntypedMessages(t *testing.T) {
	server, mockServiceClient := newTestServer(t)
	mockserver.Handle(
		server.Expect().Unary(echoMethodName),
		func(_ context.Context, request proto.Message) (proto.Message, error) {
			return &mockpb.EchoResponse{Message: string(request.ProtoReflect().Descriptor().Name())}, nil
		},
	)

	echoRequest := &mockpb.EchoRequest{}
	response, err := mockServiceClient.Echo(context.Background(), echoRequest)
	require.NoError(t, err)
	require.Equal(t, "EchoRequest", response.Message)
	require.True(t, server.AssertExpectations(t))
}
