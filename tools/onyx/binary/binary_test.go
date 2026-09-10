package binary

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/manifest"
)

func load(t *testing.T, filename string) (*Binary, error) {
	t.Helper()
	m := &onyxpb.MainManifest{}
	require.NoError(t, manifest.Load("tools/onyx/binary/testdata/"+filename, m))
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
	require.Equal(t, []string{"b-service", "a-service", "a-processor", "d-processor"}, names(b.Servers))

	// One a-service instance is shared by the grpc server and the processor.
	require.Len(t, b.Services, 3)
	require.Equal(t, []string{"a-service", "a-processor"}, names(b.Services[0].Servers))

	require.Len(t, b.GRPCClients, 3)
	inProcess, remote, processorOnly := b.GRPCClients[0], b.GRPCClients[1], b.GRPCClients[2]
	require.Equal(t, "b-service", inProcess.Name)
	require.Equal(t, "b-service", inProcess.Server.GetName())
	require.Equal(t, "b-service", inProcess.Service.GetName())
	// a-service health-checks it; b-service's own dependency on itself does not.
	require.True(t, inProcess.HealthChecked)
	require.Equal(t, "c-service", remote.Name)
	require.Nil(t, remote.Server)
	// d-processor alone holds it, and a processor reports its own health, not its dependencies'.
	require.Equal(t, "a-service", processorOnly.Name)
	require.False(t, processorOnly.HealthChecked)

	source, err := Generate(b)
	require.NoError(t, err)
	golden := "tools/onyx/binary/testdata/main.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(golden, source, 0o644))
	}
	expected, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(source))
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
