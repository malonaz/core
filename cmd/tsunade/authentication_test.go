package main_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	aipb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
)

const apiKeyHeader = "tsunade-api-key"

// What each caller may call: the roles sgpt and mailbot held under v1, method for method.
var callerToMethodSet = map[string]map[string]bool{
	"sgpt": methodSet(
		"ListModels",
		"CreateMessage", "UpdateMessage", "DeleteMessage", "ListMessages", "GetMessage", "GenerateMessage", "StreamGenerateMessage",
		"CreateChat", "UpdateChat", "DeleteChat", "ListChats", "GetChat",
	),
	"mailbot": methodSet(
		"CreateChat", "UpdateChat", "DeleteChat", "ListChats", "GetChat",
		"CreateMessage", "UpdateMessage", "DeleteMessage", "ListMessages", "GetMessage", "GenerateMessage", "StreamGenerateMessage",
		"SpeechToText", "SpeechToTextStream",
		"TextToText", "TextToTextStream",
	),
	"ai-service": methodSet(),
	"anonymous":  methodSet(),
}

func methodSet(methods ...string) map[string]bool {
	set := map[string]bool{}
	for _, method := range methods {
		set["/"+aipb.AiService_ServiceDesc.ServiceName+"/"+method] = true
	}
	return set
}

// Every caller against every AiService method: the API key clients reach their roles' methods and nothing else;
// the service's own account and an anonymous caller reach nothing.
func TestAuthentication(t *testing.T) {
	opts := &authentication.Opts{
		Config:       "cmd/tsunade/authentication/authentication.jsonnet",
		APIKeys:      "sgpt:sgpt-key,mailbot:mailbot-key",
		APIKeyHeader: apiKeyHeader,
		ServerSecret: "server-secret",
	}
	interceptor, err := authentication.NewInterceptor(context.Background(), opts)
	require.NoError(t, err)
	asAIService, err := interceptor.As("ai-service")
	require.NoError(t, err)

	callerToMetadata := func(method string) map[string]metadata.MD {
		ctx := credentials.NewContextWithRequestInfo(asAIService(context.Background()), credentials.RequestInfo{Method: method})
		serverToken, err := authentication.Credentials().GetRequestMetadata(ctx)
		require.NoError(t, err)
		return map[string]metadata.MD{
			"sgpt":       metadata.Pairs(apiKeyHeader, "sgpt-key"),
			"mailbot":    metadata.Pairs(apiKeyHeader, "mailbot-key"),
			"ai-service": metadata.New(serverToken),
			"anonymous":  {},
		}
	}
	methods := aipb.AiService_ServiceDesc.Methods
	var fullMethods []string
	for _, method := range methods {
		fullMethods = append(fullMethods, "/"+aipb.AiService_ServiceDesc.ServiceName+"/"+method.MethodName)
	}
	for _, stream := range aipb.AiService_ServiceDesc.Streams {
		fullMethods = append(fullMethods, "/"+aipb.AiService_ServiceDesc.ServiceName+"/"+stream.StreamName)
	}
	for _, fullMethod := range fullMethods {
		for caller, md := range callerToMetadata(fullMethod) {
			ctx := metadata.NewIncomingContext(context.Background(), md)
			handler := func(context.Context, any) (any, error) { return nil, nil }
			_, err := interceptor.Unary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
			want := codes.PermissionDenied
			if caller == "anonymous" {
				want = codes.Unauthenticated
			}
			if callerToMethodSet[caller][fullMethod] {
				want = codes.OK
			}
			require.Equal(t, want, grpcstatus.Code(err), "%s calling %s: %v", caller, fullMethod, err)
		}
	}
}
