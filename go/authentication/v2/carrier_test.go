package authentication_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarymodelpb "github.com/malonaz/core/genproto/test/library/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
)

// library serves GetBook, recording the session and metadata each call reached it with.
type library struct {
	librarypb.UnimplementedLibraryServiceServer
	mutex sync.Mutex
	seen  []*handled
}

func (l *library) GetBook(ctx context.Context, request *librarypb.GetBookRequest) (*librarymodelpb.Book, error) {
	session, err := authentication.GetSession(ctx)
	if err != nil {
		return nil, err
	}
	md, _ := metadata.FromIncomingContext(ctx)
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.seen = append(l.seen, &handled{session: session, metadata: md})
	return &librarymodelpb.Book{Name: request.GetName()}, nil
}

func (l *library) last() *handled {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.seen[len(l.seen)-1]
}

// gateway answers Echo and EchoStream by calling the library with its own context: carrying the call's session.
type gateway struct {
	mockpb.UnimplementedMockServiceServer
	libraryClient librarypb.LibraryServiceClient
}

func (g *gateway) Echo(ctx context.Context, request *mockpb.EchoRequest) (*mockpb.EchoResponse, error) {
	getBookRequest := &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"}
	if _, err := g.libraryClient.GetBook(ctx, getBookRequest); err != nil {
		return nil, err
	}
	return &mockpb.EchoResponse{Message: request.GetMessage()}, nil
}

func (g *gateway) EchoStream(request *mockpb.EchoRequest, stream mockpb.MockService_EchoStreamServer) error {
	getBookRequest := &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"}
	if _, err := g.libraryClient.GetBook(stream.Context(), getBookRequest); err != nil {
		return err
	}
	return stream.Send(&mockpb.EchoResponse{Message: request.GetMessage()})
}

// platform is two servers of one binary on unix sockets: the gateway, serving MockService as the gateway account,
// calls the library, serving LibraryService and reflection as the worker account, over a connection carrying
// server tokens. Outside clients dial the gateway without them.
type platform struct {
	interceptor   *authentication.Interceptor
	library       *library
	gatewaySocket string
	librarySocket string
}

func newPlatform(t testing.TB, f *fixture) *platform {
	t.Helper()
	f.configuration.Roles = append(f.configuration.Roles,
		&authenticationpb.Role{Id: "streamer", Rules: []*authenticationpb.Rule{{Methods: []string{echoStreamMethod}, ConditionCel: `request.message == "open"`}}},
		&authenticationpb.Role{Id: "reflector", Rules: []*authenticationpb.Rule{{Methods: []string{reflectionMethod}, ConditionCel: `principal.subject == "serviceAccounts/worker"`}}},
	)
	f.configuration.ServiceAccounts[2].Grants = append(f.configuration.ServiceAccounts[2].Grants,
		&authenticationpb.Grant{Role: "streamer", Scope: "*"},
		&authenticationpb.Grant{Role: "reflector", Scope: "*"},
	)
	interceptor := f.interceptor(t)
	require.NoError(t, interceptor.Serve("gateway", mockService))
	require.NoError(t, interceptor.Serve("worker", libraryService, reflectionService))

	// Short, as a socket path is capped at 108 bytes.
	directory, err := os.MkdirTemp("", "av2")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(directory) })
	p := &platform{
		interceptor:   interceptor,
		library:       &library{},
		gatewaySocket: filepath.Join(directory, "gateway.socket"),
		librarySocket: filepath.Join(directory, "library.socket"),
	}
	libraryServer := p.newServer()
	librarypb.RegisterLibraryServiceServer(libraryServer, p.library)
	reflection.RegisterV1(libraryServer)
	p.serve(t, libraryServer, p.librarySocket)

	gatewayServer := p.newServer()
	mockpb.RegisterMockServiceServer(gatewayServer, &gateway{libraryClient: librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, true))})
	p.serve(t, gatewayServer, p.gatewaySocket)
	return p
}

func (p *platform) newServer() *grpc.Server {
	return grpc.NewServer(grpc.ChainUnaryInterceptor(p.interceptor.Unary()), grpc.ChainStreamInterceptor(p.interceptor.Stream()))
}

func (p *platform) serve(t testing.TB, server *grpc.Server, socket string) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
}

// dial connects to a server, with the platform's per-RPC credentials when inside.
func (p *platform) dial(t testing.TB, socket string, inside bool) *grpc.ClientConn {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if inside {
		options = append(options, grpc.WithPerRPCCredentials(authentication.Credentials()))
	}
	connection, err := grpc.NewClient("unix://"+socket, options...)
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close() })
	return connection
}

func (p *platform) as(t testing.TB, account string) context.Context {
	t.Helper()
	as, err := p.interceptor.As(account)
	require.NoError(t, err)
	return as(context.Background())
}

// The carrier rule of design section 9 holds what v1 did between servers: a session one server carries is believed,
// whoever its principal, the anonymous one included; a service calling as itself is held to its own grants.
func TestCarrier(t *testing.T) {
	f := newFixture(t)
	p := newPlatform(t, f)
	outside := mockpb.NewMockServiceClient(p.dial(t, p.gatewaySocket, false))
	echoRequest := &mockpb.EchoRequest{Message: "open"}

	t.Run("a token holder's call is carried", func(t *testing.T) {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Join(
			bearer(f.token(t, map[string]any{"shelf": "shelves/1", "act": map[string]any{"sub": "jdoe@example.com"}})),
			metadata.Pairs("x-client-platform", "ios", "x-client-version", "1.2.3", "x-forwarded-for", "203.0.113.7"),
		))
		_, err := outside.Echo(ctx, echoRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "shelves/1", seen.session.GetPrincipal().GetAttributes().GetFields()["shelf"].GetStringValue())
		require.Equal(t, "jdoe@example.com", seen.session.GetActor().GetSubject())
		require.Equal(t, "serviceAccounts/gateway", seen.session.GetCaller().GetSubject())
		require.Len(t, seen.session.GetGrants(), 2)
		require.Equal(t, "ios", seen.session.GetClient().GetPlatform())
		require.Equal(t, "203.0.113.7", seen.session.GetClient().GetIpAddress())
		require.EqualValues(t, 2, seen.session.GetClient().GetVersion().GetMinor())
		require.Empty(t, seen.metadata.Get("x-server-token"))
	})
	t.Run("an anonymous call is carried", func(t *testing.T) {
		_, err := outside.Echo(context.Background(), echoRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Nil(t, seen.session.GetPrincipal())
		require.Equal(t, "serviceAccounts/gateway", seen.session.GetCaller().GetSubject())
	})
	t.Run("an api key of an internal account is not a carrier", func(t *testing.T) {
		// The worker's carrier rule needs a caller, which a call from outside never has.
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, false))
		ctx := metadata.NewOutgoingContext(context.Background(), apiKey("key-3"))
		_, err := library.GetBook(ctx, getBookRequest)
		require.NoError(t, err, "worker holds reader")
		listBooksRequest := &librarypb.ListBooksRequest{Parent: "shelves/1"}
		_, err = library.ListBooks(ctx, listBooksRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})
	t.Run("a service calling as itself is held to its own grants", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, true))
		_, err := library.GetBook(p.as(t, "gateway"), getBookRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "gateway holds only carrier: %v", err)

		_, err = library.GetBook(p.as(t, "worker"), getBookRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Equal(t, "serviceAccounts/worker", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "serviceAccounts/worker", seen.session.GetCaller().GetSubject())
	})
	t.Run("as replaces the session a context carries", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, true))
		as, err := p.interceptor.As("worker")
		require.NoError(t, err)
		ctx := as(authenticationtest.NewContext(context.Background(), &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}}))
		_, err = library.GetBook(ctx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/worker", p.library.last().session.GetPrincipal().GetSubject())
	})
	t.Run("a server stream, with its request peeked, carries its session", func(t *testing.T) {
		gatewayClient := mockpb.NewMockServiceClient(p.dial(t, p.gatewaySocket, true))
		stream, err := gatewayClient.EchoStream(p.as(t, "worker"), echoRequest)
		require.NoError(t, err)
		response, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, "open", response.GetMessage())
		seen := p.library.last()
		require.Equal(t, "serviceAccounts/worker", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "serviceAccounts/gateway", seen.session.GetCaller().GetSubject())

		stream, err = gatewayClient.EchoStream(p.as(t, "worker"), &mockpb.EchoRequest{Message: "closed"})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})
	t.Run("a bidirectional stream", func(t *testing.T) {
		reflectionClient := reflectionpb.NewServerReflectionClient(p.dial(t, p.librarySocket, true))
		stream, err := reflectionClient.ServerReflectionInfo(p.as(t, "worker"))
		require.NoError(t, err)
		listServicesRequest := &reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{}}
		require.NoError(t, stream.Send(listServicesRequest))
		_, err = stream.Recv()
		require.NoError(t, err)

		stream, err = reflectionClient.ServerReflectionInfo(p.as(t, "gateway"))
		require.NoError(t, err)
		require.NoError(t, stream.Send(listServicesRequest))
		_, err = stream.Recv()
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})
	t.Run("a context with neither a session nor an account calls anonymously", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, true))
		_, err := library.GetBook(context.Background(), getBookRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
	t.Run("a session no account serves cannot be carried", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket, true))
		ctx := authenticationtest.NewContext(context.Background(), &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}})
		_, err := library.GetBook(ctx, getBookRequest)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
}

// As and Serve refuse an account the configuration lacks, and one with keys of its own, which no process here signs as.
func TestServeAndAs(t *testing.T) {
	interceptor := newFixture(t).interceptor(t)
	_, err := interceptor.As("ghost")
	require.ErrorContains(t, err, "unknown service account")
	require.ErrorContains(t, interceptor.Serve("outsider", mockService), "cannot sign as it")
}
