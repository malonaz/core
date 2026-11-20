package authentication_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarymodelpb "github.com/malonaz/core/genproto/test/library/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	coregrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/grpcwebproxy"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler/endpoints"
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
	// detached is the last Echo's context, kept past its call as a background goroutine keeps it.
	detached atomic.Value
}

func (g *gateway) Echo(ctx context.Context, request *mockpb.EchoRequest) (*mockpb.EchoResponse, error) {
	g.detached.Store(context.WithoutCancel(ctx))
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
// calls the library, serving LibraryService and reflection as the worker account. Every connection is core's, with
// nothing added: the context of a call decides what it carries.
type platform struct {
	interceptor   *authentication.Interceptor
	library       *library
	gateway       *gateway
	directory     string
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
		directory:     directory,
		gatewaySocket: filepath.Join(directory, "gateway.socket"),
		librarySocket: filepath.Join(directory, "library.socket"),
	}
	libraryServer := p.newServer()
	librarypb.RegisterLibraryServiceServer(libraryServer, p.library)
	reflection.RegisterV1(libraryServer)
	p.serve(t, libraryServer, p.librarySocket)

	p.gateway = &gateway{libraryClient: librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))}
	gatewayServer := p.newServer()
	mockpb.RegisterMockServiceServer(gatewayServer, p.gateway)
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

// dial connects to a server as any code does: with core's connection, and nothing more.
func (p *platform) dial(t testing.TB, socket string) *grpc.ClientConn {
	t.Helper()
	connection, err := coregrpc.NewConnection(&coregrpc.ClientOpts{SocketPath: socket, DisableTLS: true}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, connection.Connect(context.Background()))
	t.Cleanup(func() { connection.Close() })
	return connection.Get()
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
	outside := mockpb.NewMockServiceClient(p.dial(t, p.gatewaySocket))
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
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))
		ctx := metadata.NewOutgoingContext(context.Background(), apiKey("key-3"))
		_, err := library.GetBook(ctx, getBookRequest)
		require.NoError(t, err, "worker holds reader")
		listBooksRequest := &librarypb.ListBooksRequest{Parent: "shelves/1"}
		_, err = library.ListBooks(ctx, listBooksRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})
	t.Run("a service calling as itself is held to its own grants", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))
		_, err := library.GetBook(p.as(t, "gateway"), getBookRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "gateway holds only carrier: %v", err)

		_, err = library.GetBook(p.as(t, "worker"), getBookRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Equal(t, "serviceAccounts/worker", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "serviceAccounts/worker", seen.session.GetCaller().GetSubject())
	})
	t.Run("as replaces the session a context carries", func(t *testing.T) {
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))
		as, err := p.interceptor.As("worker")
		require.NoError(t, err)
		ctx := as(authenticationtest.NewContext(context.Background(), &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}}))
		_, err = library.GetBook(ctx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/worker", p.library.last().session.GetPrincipal().GetSubject())
	})
	t.Run("a server stream, with its request peeked, carries its session", func(t *testing.T) {
		gatewayClient := mockpb.NewMockServiceClient(p.dial(t, p.gatewaySocket))
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
		reflectionClient := reflectionpb.NewServerReflectionClient(p.dial(t, p.librarySocket))
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
		library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))
		_, err := library.GetBook(context.Background(), getBookRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
	t.Run("a context kept past its call still carries its session", func(t *testing.T) {
		ctx := metadata.NewOutgoingContext(context.Background(), bearer(f.token(t, nil)))
		_, err := outside.Echo(ctx, echoRequest)
		require.NoError(t, err)
		_, err = p.gateway.libraryClient.GetBook(p.gateway.detached.Load().(context.Context), getBookRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "serviceAccounts/gateway", seen.session.GetCaller().GetSubject())
	})
	t.Run("a session no account serves cannot be carried", func(t *testing.T) {
		// A server of the platform whose services no account serves.
		unserved := grpc.NewServer(grpc.ChainUnaryInterceptor(newFixture(t).interceptor(t).Unary()))
		mockpb.RegisterMockServiceServer(unserved, &gateway{libraryClient: librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))})
		socket := filepath.Join(p.directory, "unserved.socket")
		p.serve(t, unserved, socket)
		_, err := mockpb.NewMockServiceClient(p.dial(t, socket)).Echo(context.Background(), echoRequest)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
	t.Run("a server of another binary receives the token too, addressed to the service called", func(t *testing.T) {
		received := make(chan metadata.MD, 1)
		record := func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			received <- md
			return handler(ctx, request)
		}
		remote := grpc.NewServer(grpc.UnaryInterceptor(record))
		mockpb.RegisterMockServiceServer(remote, &mockpb.UnimplementedMockServiceServer{})
		socket := filepath.Join(p.directory, "remote.socket")
		p.serve(t, remote, socket)
		_, err := mockpb.NewMockServiceClient(p.dial(t, socket)).Echo(p.as(t, "worker"), echoRequest)
		require.True(t, status.HasCode(err, codes.Unimplemented), "%v", err)
		serverTokens := (<-received).Get("x-server-token")
		require.Len(t, serverTokens, 1)
		token, err := jwt.Parse([]byte(serverTokens[0]), jwt.WithKey(jwa.HS256, []byte(serverSecret)), jwt.WithAudience(mockService))
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/worker", token.Issuer())
	})
	t.Run("the scheduler dispatcher discovers and delivers as its account", func(t *testing.T) {
		// Its endpoints are dialed by go/scheduler/endpoints, and its calls made on the context its Start was given.
		pool := endpoints.NewPool()
		t.Cleanup(pool.Close)
		ctx := p.as(t, "worker")
		endpoint := "unix:" + p.librarySocket
		_, err := pool.Discover(ctx, endpoint)
		require.NoError(t, err)
		connection, err := pool.Acquire(ctx, endpoint)
		require.NoError(t, err)
		require.NoError(t, connection.Get().Invoke(ctx, getBookMethod, getBookRequest, &librarymodelpb.Book{}))
		require.Equal(t, "serviceAccounts/worker", p.library.last().session.GetPrincipal().GetSubject())
	})
}

// As and Serve refuse an account the configuration lacks, and one with keys of its own, which no process here signs as.
func TestServeAndAs(t *testing.T) {
	interceptor := newFixture(t).interceptor(t)
	_, err := interceptor.As("ghost")
	require.ErrorContains(t, err, "unknown service account")
	require.ErrorContains(t, interceptor.Serve("outsider", mockService), "cannot sign as it")
}

// The edge relays the outside caller's own credential and nothing more: neither the grpc-gateway's connection nor the
// grpc-web proxy's carries a server token, so the library sees the caller itself, from no other server.
func TestEdgeRelaysOnlyTheOutsideCredential(t *testing.T) {
	f := newFixture(t)
	p := newPlatform(t, f)
	ctx := context.Background()
	token := f.token(t, nil)
	library := &coregrpc.ClientOpts{SocketPath: p.librarySocket, DisableTLS: true}
	requireRelayed := func(t *testing.T) {
		seen := p.library.last()
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		require.Nil(t, seen.session.GetCaller())
	}

	t.Run("grpc-gateway", func(t *testing.T) {
		// What a generated handler does: dial the endpoint with the gateway's options, the request's headers becoming
		// the call's metadata.
		register := func(ctx context.Context, mux *runtime.ServeMux, endpoint string, options []grpc.DialOption) error {
			connection, err := grpc.NewClient(endpoint, options...)
			if err != nil {
				return err
			}
			libraryClient := librarypb.NewLibraryServiceClient(connection)
			return mux.HandlePath(http.MethodGet, "/book", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
				ctx, err := runtime.AnnotateContext(r.Context(), mux, r, getBookMethod)
				if err == nil {
					_, err = libraryClient.GetBook(ctx, getBookRequest)
				}
				if err != nil {
					runtime.HTTPError(ctx, mux, &runtime.JSONPb{}, w, r, err)
				}
			})
		}
		port := freePort(t)
		gateway := coregrpc.NewGateway(&coregrpc.GatewayOpts{Port: port}, library, nil, nil, []coregrpc.RegisterHandler{register})
		require.NoError(t, gateway.Listen(ctx))
		go gateway.Serve(ctx)
		t.Cleanup(func() { gateway.Stop() })
		request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/book", port), nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		requireRelayed(t)
	})
	t.Run("grpc-web proxy", func(t *testing.T) {
		port := freePort(t)
		proxy := grpcwebproxy.NewServer(&grpcwebproxy.Opts{Host: "localhost", Port: port, AllowAllOrigins: true, DisableCompression: true, Backend: library}, nil, nil)
		require.NoError(t, proxy.Listen(ctx))
		go proxy.Serve(ctx)
		t.Cleanup(func() { proxy.Stop() })
		message, err := proto.Marshal(getBookRequest)
		require.NoError(t, err)
		frame := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(message)))
		request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%d%s", port, getBookMethod), bytes.NewReader(append(frame, message...)))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/grpc-web+proto")
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, err)
		require.Contains(t, string(body), getBookRequest.GetName(), "the book came back")
		requireRelayed(t)
	})
}

// freePort returns a TCP port nothing listens on.
func freePort(t testing.TB) int {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
