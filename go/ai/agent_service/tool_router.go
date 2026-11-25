package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbjson"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// toolSchemaTTL bounds how long a target's reflected schema is reused: a redeployed target may change its API.
const toolSchemaTTL = 5 * time.Minute

// toolRouter routes the methods of gRPC services to the endpoints serving them, their configured targets.
type toolRouter struct {
	serviceToEndpoint map[string]string
	connections       *connections
}

func newToolRouter(serviceToEndpoint map[string]string, connections *connections) (*toolRouter, error) {
	for service, endpoint := range serviceToEndpoint {
		if service == "" {
			return nil, fmt.Errorf("tool target %q names no service", endpoint)
		}
		if _, err := grpc.ParseClientOpts(endpoint); err != nil {
			return nil, fmt.Errorf("parsing the endpoint of tool service %s: %w", service, err)
		}
	}
	return &toolRouter{serviceToEndpoint: serviceToEndpoint, connections: connections}, nil
}

// invoke calls the method a tool stands for on its service's target as ownerCtx, with a call's arguments, and returns
// its response as JSON. The arguments speak the tool schema's JSON, friendlier to models than protojson (money as
// "USD 25.50", snake_case field masks, Go durations, intervals, nested messages sent as strings): only the method's
// descriptors translate it, so they are reflected from the target as ctx (the service), cached.
func (r *toolRouter) invoke(ctx, ownerCtx context.Context, toolMethod *aipb.ToolMethod, arguments *structpb.Struct, requestID string) (*structpb.Value, error) {
	endpoint := r.serviceToEndpoint[toolMethod.GetService()]
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connecting to %s: %v", endpoint, err).Err()
	}
	reflectionClient := grpc_reflection_v1.NewServerReflectionClient(connection.Get())
	schema, err := pbreflection.ResolveSchema(ctx, reflectionClient, pbreflection.WithMemCache("agent-service-tools:"+endpoint, toolSchemaTTL))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "resolving the schema served at %s: %v", endpoint, err).Err()
	}
	descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(toolMethod.GetMethod()))
	method, ok := descriptor.(protoreflect.MethodDescriptor)
	if err != nil || !ok {
		return nil, status.Errorf(codes.Unavailable, "%s serves no method %s", endpoint, toolMethod.GetMethod()).Err()
	}

	toolCall := &aipb.ToolUse{Arguments: arguments}
	request, err := buildToolCallMessage(pbjson.NewSchemaBuilder(schema), method.Input(), toolCall)
	if err != nil {
		return nil, err
	}
	if field := method.Input().Fields().ByName("request_id"); field != nil && field.Kind() == protoreflect.StringKind && !field.IsList() {
		request.ProtoReflect().Set(field, protoreflect.ValueOfString(requestID))
	}
	// The builder sets generated well-known types on the dynamic request: validation walks one descriptor set.
	if request, err = uniformMessage(method.Input(), request); err != nil {
		return nil, err
	}
	response, err := pbreflection.NewMethodInvoker(connection.Get()).Invoke(ownerCtx, method, request)
	if err != nil {
		return nil, err
	}
	responseBytes, err := pbutil.JSONMarshal(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling response: %v", err).Err()
	}
	content := &structpb.Value{}
	if err := pbutil.JSONUnmarshal(responseBytes, content); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling response: %v", err).Err()
	}
	return content, nil
}

// uniformMessage re-reads a message as a dynamic message of one descriptor, its nested messages included.
func uniformMessage(messageDescriptor protoreflect.MessageDescriptor, message proto.Message) (protoreflect.ProtoMessage, error) {
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling request: %v", err).Err()
	}
	dynamicMessage := dynamicpb.NewMessage(messageDescriptor)
	if err := proto.Unmarshal(messageBytes, dynamicMessage); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling request: %v", err).Err()
	}
	return dynamicMessage, nil
}

// testMethods asks a routed service's target which of its methods (full names) the session ctx carries could call
// (AuthorizationService.TestMethods), returning their full names.
func (r *toolRouter) testMethods(ctx context.Context, service string, methods []string) ([]string, error) {
	endpoint := r.serviceToEndpoint[service]
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connecting to %s: %v", endpoint, err).Err()
	}
	testMethodsRequest := &authenticationpb.TestMethodsRequest{}
	for _, method := range methods {
		testMethodsRequest.Methods = append(testMethodsRequest.Methods, "/"+service+"/"+strings.TrimPrefix(method, service+"."))
	}
	testMethodsResponse, err := authenticationpb.NewAuthorizationServiceClient(connection.Get()).TestMethods(ctx, testMethodsRequest)
	if err != nil {
		return nil, err
	}
	reachable := make([]string, 0, len(testMethodsResponse.GetMethods()))
	for _, method := range testMethodsResponse.GetMethods() {
		reachable = append(reachable, strings.Replace(strings.TrimPrefix(method, "/"), "/", ".", 1))
	}
	return reachable, nil
}
