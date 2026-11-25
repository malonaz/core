package binary

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/tools/onyx/manifest"
)

func load(t *testing.T, filename string) (*Binary, error) {
	t.Helper()
	m, err := manifest.LoadMain("tools/onyx/binary/testdata/"+filename, "")
	require.NoError(t, err)
	return Load(m, "example.com/repo")
}

func names(servers []*Server) []string {
	out := make([]string, len(servers))
	for i, server := range servers {
		out[i] = server.GetName()
	}
	return out
}

func TestLoad(t *testing.T) {
	b, err := load(t, "main.yaml")
	require.NoError(t, err)

	// b-service is dialed by a-service, so it starts first; the rest keep manifest order.
	require.Equal(t, []string{"b-service", "a-service", "a-processor", "d-processor", "d-http", "web"}, names(b.Servers))

	// One a-service instance is shared by the grpc server and the processor.
	require.Len(t, b.Services, 4)
	require.Equal(t, []string{"a-service", "a-processor"}, names(b.Services[0].Servers))

	require.Len(t, b.GRPCClients, 3)
	inProcess, remote, processorOnly := b.GRPCClients[0], b.GRPCClients[1], b.GRPCClients[2]
	require.Equal(t, "b-service", inProcess.Name)
	require.Equal(t, "b-service", inProcess.Server.GetName())
	require.Equal(t, "b-service", inProcess.Service.GetName())
	require.Equal(t, "c-service", remote.Name)
	require.Nil(t, remote.Server)
	require.Equal(t, "a-service", processorOnly.Name)

	source, err := Generate(b)
	require.NoError(t, err)
	// h-service asks to stop before the servers: with the processor, ahead of the servers' graceful stops.
	require.Contains(t, string(source), "hServiceStop := sync.OnceFunc(hServiceCleanup)")
	require.Contains(t, string(source), "lifecycle.CloseAll(dProcessorStop, hServiceStop)")
	golden := "tools/onyx/binary/testdata/main.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(golden, source, 0o644))
	}
	expected, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(source))
}

func TestGenerateProcessorOnly(t *testing.T) {
	// Nothing listens, so runServer would be declared and not used.
	b, err := load(t, "processor_only.yaml")
	require.NoError(t, err)
	source, err := Generate(b)
	require.NoError(t, err)
	require.NotContains(t, string(source), "runServer")
	require.Contains(t, string(source), "dProcessorCleanup, err := dProcessor.Start(ctx, nil)")
}

func TestGenerateHTTPOnly(t *testing.T) {
	// No gRPC server or client: core's grpc package would be imported and not used.
	b, err := load(t, "http_only.yaml")
	require.NoError(t, err)
	source, err := Generate(b)
	require.NoError(t, err)
	require.NotContains(t, string(source), `"github.com/malonaz/core/go/grpc"`)
	require.Contains(t, string(source), "IHttpHTTP *http.Opts")
}

func TestGenerateDeployment(t *testing.T) {
	// A ServiceManifest's deployment is the http_only binary serving that service.
	m, err := manifest.LoadMain("tools/onyx/binary/testdata/i_deployment.yaml", "//i/i_service:manifest")
	require.NoError(t, err)
	b, err := Load(m, "example.com/repo")
	require.NoError(t, err)
	source, err := Generate(b)
	require.NoError(t, err)

	expected, err := load(t, "http_only.yaml")
	require.NoError(t, err)
	expectedSource, err := Generate(expected)
	require.NoError(t, err)
	require.Equal(t, string(expectedSource), string(source))
}

func TestLoadRejectsMainWithoutServiceManifest(t *testing.T) {
	_, err := manifest.LoadMain("tools/onyx/binary/testdata/http_no_service.yaml", "")
	require.ErrorContains(t, err, "servers[0].http.services: value must contain at least 1 item")
}

func TestLoadRejectsNeitherManifest(t *testing.T) {
	// A file that is neither reports why it is no MainManifest.
	_, err := manifest.LoadMain("tools/onyx/binary/testdata/neither.yaml", "//i/i_service:manifest")
	require.ErrorContains(t, err, `unknown field "codegen"`)
}

func TestLoadRejectsServiceWithoutDeployment(t *testing.T) {
	_, err := manifest.LoadMain("tools/onyx/binary/testdata/i_service.yaml", "//i/i_service:manifest")
	require.ErrorContains(t, err, "service i-service has no deployment")
}

func TestLoadRejectsServiceCycle(t *testing.T) {
	_, err := load(t, "cyclic.yaml")
	require.ErrorContains(t, err, "services depend on each other: a-service -> b-service -> a-service")
}

func TestLoadRejectsIdentifierCollision(t *testing.T) {
	// A server named c-service claims opts.CServiceGRPC, as does the remote c-service client.
	_, err := load(t, "collision.yaml")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "opts.CServiceGRPC"), err.Error())
}

func TestLoadRejectsSameNameClients(t *testing.T) {
	// Two c-service clients on different protos would both be cServiceClient.
	_, err := load(t, "same_name_clients.yaml")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "cServiceClient"), err.Error())
}

func TestLoadNamedClient(t *testing.T) {
	// Naming one of them tells the two apart.
	b, err := load(t, "named_clients.yaml")
	require.NoError(t, err)
	require.Len(t, b.GRPCClients, 2)
	require.Equal(t, "c-service", b.GRPCClients[0].Name)
	require.Equal(t, "c2-service", b.GRPCClients[1].Name)
	require.Equal(t, "CService", b.GRPCClients[1].GoName)
}

func TestLoadLongrunning(t *testing.T) {
	b, err := load(t, "lro.yaml")
	require.NoError(t, err)
	server := b.Servers[0]
	// A gateway method's jobs are the proxied method's.
	require.Equal(t, []string{"/test.lro.v1.LroService/Import", "/test.other.v1.OtherService/Import"}, server.LongrunningMethods)
	require.Equal(t, "scheduler-service", server.SchedulerClient.Name)
	source, err := Generate(b)
	require.NoError(t, err)
	require.Contains(t, string(source), "longrunningpb.RegisterOperationsServer(server.Raw, longrunning.NewServer(schedulerServiceClient, []string{")
	// lro-client asked for lro-service's operations: the Operations client shares its connection.
	require.Contains(t, string(source), "lroServiceOperationsClient := longrunningpb.NewOperationsClient(lroServiceConnection.Get())")
	require.Contains(t, string(source), "lroclient.New(opts.LroClient, lroServiceClient, lroServiceOperationsClient)")
}

func TestLoadRejectsOperationsWithoutLongrunning(t *testing.T) {
	_, err := load(t, "lro_c_client.yaml")
	require.ErrorContains(t, err, "asks for operations but test.c.v1.CService has no method returning google.longrunning.Operation")
}

func TestLoadLongrunningGateway(t *testing.T) {
	b, err := load(t, "lro_gateway.yaml")
	require.NoError(t, err)
	gateway := b.Servers[1]
	require.Equal(t, "lro-gateway", gateway.GetName())
	// The gateway proxies lro-service's method and reads its operations through the binary's
	// scheduler client, which only lro-service declares.
	require.Equal(t, []string{"/test.lro.v1.LroService/Import"}, gateway.LongrunningMethods)
	require.Equal(t, "scheduler-service", gateway.SchedulerClient.Name)
}

func TestLoadRejectsLongrunningWithoutScheduler(t *testing.T) {
	_, err := load(t, "lro_unscheduled.yaml")
	require.ErrorContains(t, err, "no service of the binary has a grpc_client on malonaz.scheduler.scheduler_service.v1.SchedulerService")
}

// A binary on go/authentication/v2: one interceptor built before anything dials, the account serving each gRPC
// service named before any server serves, and As handed to every service's Start.
func TestGenerateAuthentication(t *testing.T) {
	b, err := load(t, "authentication.yaml")
	require.NoError(t, err)
	source, err := Generate(b)
	require.NoError(t, err)
	golden := "tools/onyx/binary/testdata/authentication.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(golden, source, 0o644))
	}
	expected, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(source))

	main := string(source)
	require.Contains(t, main, `Authentication *authentication.Opts `+"`"+`group:"Authentication" namespace:"authentication" env-namespace:"AUTHENTICATION"`+"`")
	construct := strings.Index(main, "authentication.NewInterceptor(ctx, opts.Authentication)")
	firstConnect := strings.Index(main, ":= connect(")
	firstListen := strings.Index(main, ".Listen(ctx)")
	require.True(t, construct >= 0 && construct < firstConnect, "the interceptor is built before anything dials")
	for _, serve := range []string{
		`authenticationInterceptor.Serve("a-service", aservicepb.AService_ServiceDesc.ServiceName)`,
		`authenticationInterceptor.Serve("b-service", bservicepb.BService_ServiceDesc.ServiceName)`,
		`authenticationInterceptor.Serve("lro-service", lroservicepb.LroService_ServiceDesc.ServiceName)`,
		`authenticationInterceptor.Serve("scheduler-service", schedulerservicepb.SchedulerService_ServiceDesc.ServiceName)`,
		`authenticationInterceptor.Serve("scheduler-service", longrunningpb.Operations_ServiceDesc.ServiceName)`,
	} {
		index := strings.Index(main, serve)
		require.True(t, index >= 0 && index < firstListen, "%s, before any server listens", serve)
	}
	// Clients are dialed as in a binary without it: the context of each call, not its connection, carries the session.
	require.Contains(t, main, "connect(opts.BServiceGRPC.ClientOpts())")
	require.Contains(t, main, "connect(opts.CServiceGRPC)")
	require.NotContains(t, main, "WithOptions(")
	for _, service := range []string{"aService", "bService", "lroService", "schedulerService", "dProcessor", "hService"} {
		require.Contains(t, main, service+"WithServiceAccount, err := authenticationInterceptor.As(")
		require.Contains(t, main, service+".Start(ctx, "+service+"WithServiceAccount)")
	}
	require.Equal(t, 4, strings.Count(main, "authenticationInterceptor.Unary(),"))
	require.Equal(t, 4, strings.Count(main, "authenticationInterceptor.Stream(),"))
}

func TestLoadRejectsServerWithoutAuthentication(t *testing.T) {
	_, err := load(t, "authentication_partial.yaml")
	require.ErrorContains(t, err, "server b-service does not list INTERCEPTOR_AUTHENTICATION, which every gRPC server of the binary must")
}
