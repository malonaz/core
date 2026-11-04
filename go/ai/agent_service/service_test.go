package agent_service

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/gengo/ai/agent/store"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
	"github.com/malonaz/core/go/uuid"
)

// agentMigrationsDirectory holds the agent database's migrations, provided to the test as data.
const agentMigrationsDirectory = "go/ai/agent/migrations"

// testPostgresClient is connected to the test Postgres, migrated, shared by every test: isolate tests by organization.
var testPostgresClient *postgres.Client

func TestMain(m *testing.M) {
	testserver.RunWithPostgres(context.Background(), m, slog.Default(), &testPostgresClient, nil, "", os.ReadFile, agentMigrationsDirectory)
}

// newTestOpts returns opts short enough for tests to watch heartbeats expire and refusals lapse.
func newTestOpts() *Opts {
	return &Opts{HeartbeatTimeout: 2 * time.Second, SweepInterval: 100 * time.Millisecond, RefusalCooldown: time.Second}
}

// newTestStore returns a store over the test Postgres.
func newTestStore() *store.Store {
	return store.New(testPostgresClient)
}

// newTestService returns a started Service over the test Postgres and a fake AiService, stopped when the test ends.
// Its sweeper spans every organization, and executor commands only reach sessions of the service that sent them:
// never run tests that start a service in parallel.
func newTestService(t *testing.T) *Service {
	t.Helper()
	service, err := New(newTestOpts(), newTestStore(), newFakeAiServiceClient())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := service.Start(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		stop()
		cancel()
	})
	return service
}

// newTestOrganization returns a fresh organization name, isolating a test's rows in the shared database.
func newTestOrganization(t *testing.T) string {
	t.Helper()
	organizationRn := &agentpb.OrganizationRn{Organization: aip.NewSystemGeneratedBase32ResourceID()}
	return organizationRn.String()
}

// testAiService returns the fake AiService client of a service built by newTestService.
func testAiService(service *Service) *fakeAiServiceClient {
	return service.aiServiceClient.(*fakeAiServiceClient)
}

// fakeAiServiceClient is an in-memory AiService. Only CreateChat is implemented: other methods panic.
type fakeAiServiceClient struct {
	aiservicepb.AiServiceClient

	mutex              sync.Mutex
	requestIDToChat    map[string]*aipb.Chat
	createChatRequests []*aiservicepb.CreateChatRequest
}

func newFakeAiServiceClient() *fakeAiServiceClient {
	return &fakeAiServiceClient{requestIDToChat: map[string]*aipb.Chat{}}
}

// CreateChat records the request and creates a chat named after its request id: replaying a request id returns
// the same chat.
func (c *fakeAiServiceClient) CreateChat(ctx context.Context, request *aiservicepb.CreateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.createChatRequests = append(c.createChatRequests, proto.Clone(request).(*aiservicepb.CreateChatRequest))
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	if chat, ok := c.requestIDToChat[requestID]; ok {
		return proto.Clone(chat).(*aipb.Chat), nil
	}
	chatID := request.ChatId
	if chatID == "" {
		chatID = uuid.NewV5(aip.MustGetUUIDNamespace(&aipb.Chat{}), requestID).String()
	}
	userRn, err := aipb.ParseUserRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	chat := proto.Clone(request.Chat).(*aipb.Chat)
	chat.Name = userRn.ChatRn(chatID).String()
	c.requestIDToChat[requestID] = chat
	return proto.Clone(chat).(*aipb.Chat), nil
}

// CreateChatRequests returns the CreateChat requests received so far.
func (c *fakeAiServiceClient) CreateChatRequests() []*aiservicepb.CreateChatRequest {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return slices.Clone(c.createChatRequests)
}

func TestFakeAiServiceClient_CreateChatIsIdempotent(t *testing.T) {
	service := newTestService(t)
	userRn := &aipb.UserRn{Organization: "organization", User: "user"}
	createChatRequest := &aiservicepb.CreateChatRequest{
		Parent:    userRn.String(),
		Chat:      &aipb.Chat{},
		RequestId: uuid.MustNewV7().String(),
	}
	chat, err := service.aiServiceClient.CreateChat(context.Background(), createChatRequest)
	require.NoError(t, err)
	replayedChat, err := service.aiServiceClient.CreateChat(context.Background(), createChatRequest)
	require.NoError(t, err)
	require.Equal(t, chat.Name, replayedChat.Name)
	require.Len(t, testAiService(service).CreateChatRequests(), 2)
}
