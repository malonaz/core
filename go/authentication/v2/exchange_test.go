package authentication_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	exchangeTokenMethod = "/malonaz.authentication.v2.TokenService/ExchangeToken"
	exchangeSecret      = "exchange-secret-0123456789abcdef"

	alice = "organizations/1/users/alice"
	// bob also holds exchanger, so that his exchanged token passes the rule and meets the service's refusal.
	bob = "organizations/1/users/bob"
	// eve's claims name `exp`, which the exchange writes.
	eve = "organizations/1/users/eve"
	// dan's claims cannot be read: the host fails.
	dan = "organizations/1/users/dan"
)

// subjectsFunc is a host answering claims with a function.
type subjectsFunc func(ctx context.Context, issuer, subject string) (map[string]any, error)

func (f subjectsFunc) Claims(ctx context.Context, issuer, subject string) (map[string]any, error) {
	return f(ctx, issuer, subject)
}

// reader is the grant the host's claims give its subjects.
var reader = map[string]any{"role": "reader", "scope": "organizations/1"}

// host knows alice, bob, eve and dan.
var host = subjectsFunc(func(_ context.Context, _, subject string) (map[string]any, error) {
	switch subject {
	case alice:
		return map[string]any{"organization": "organizations/1", "grants": []any{reader}}, nil
	case bob:
		return map[string]any{"organization": "organizations/1", "grants": []any{reader, map[string]any{"role": "exchanger", "scope": "*"}}}, nil
	case eve:
		return map[string]any{"organization": "organizations/1", "grants": []any{reader}, "exp": 4102444800}, nil
	case dan:
		return nil, status.Errorf(codes.Unavailable, "directory unavailable").Err()
	}
	return nil, status.Errorf(codes.NotFound, "no subject %s", subject).Err()
})

// exchangeKeys sign the tokens of the issuers withExchange adds.
var exchangeKeys = map[string]string{"agent": mustSymmetricKey(exchangeSecret), "kenji": mustSymmetricKey(exchangeSecret)}

// withExchange adds two issuers exchanged for with ten minute tokens: agent, whose grants are those its claims list,
// and kenji, whose actor is an `impersonator` claim, never `act`. The worker holds
// exchanger, which exchanges for actors under agents/.
func withExchange(f *fixture) {
	keys := &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(mustSymmetricKey(exchangeSecret))}
	f.configuration.Issuers = append(f.configuration.Issuers,
		&authenticationpb.Issuer{
			Id:               "agent",
			Issuer:           "agent",
			Audience:         "agent",
			Keys:             keys,
			GrantsCel:        `claims.grants.map(grant, Grant{role: grant.role, scope: grant.scope})`,
			ExchangeLifetime: durationpb.New(10 * time.Minute),
		},
		&authenticationpb.Issuer{
			Id:               "kenji",
			Issuer:           "kenji",
			Audience:         "kenji",
			Keys:             keys,
			ActorCel:         `"impersonator" in claims ? claims.impersonator : ""`,
			GrantsCel:        `[Grant{role: "reader", scope: claims.organization}]`,
			ExchangeLifetime: durationpb.New(10 * time.Minute),
		},
	)
	f.configuration.Roles = append(f.configuration.Roles, &authenticationpb.Role{
		Id:    "exchanger",
		Rules: []*authenticationpb.Rule{{Methods: []string{exchangeTokenMethod}, ConditionCel: `request.actor.startsWith("agents/")`}},
	})
	f.configuration.ServiceAccounts[2].Grants = append(f.configuration.ServiceAccounts[2].Grants, &authenticationpb.Grant{Role: "exchanger", Scope: "*"})
}

func newTokenService(t testing.TB, f *fixture, subjects authentication.Subjects) *authentication.TokenService {
	t.Helper()
	service, err := authentication.NewTokenService(context.Background(), f.configuration, subjects, exchangeKeys)
	require.NoError(t, err)
	return service
}

func exchangeRequest(subject string) *authenticationpb.ExchangeTokenRequest {
	return &authenticationpb.ExchangeTokenRequest{Issuer: "agent", Subject: subject, Actor: "agents/1"}
}

// A key is given for exactly the issuers exchanged for.
func TestNewTokenService(t *testing.T) {
	f := newFixture(t)
	withExchange(f)
	ctx := context.Background()
	_, err := authentication.NewTokenService(ctx, f.configuration, host, map[string]string{"agent": exchangeKeys["agent"]})
	require.ErrorContains(t, err, `issuer "kenji" is exchanged for, but no key is given`)

	issuerIDToKey := map[string]string{"test": mustSymmetricKey(issuerSecret)}
	for issuerID, key := range exchangeKeys {
		issuerIDToKey[issuerID] = key
	}
	_, err = authentication.NewTokenService(ctx, f.configuration, host, issuerIDToKey)
	require.ErrorContains(t, err, `a key is given for issuer "test", which has no exchange_lifetime`)
}

func TestExchangeToken(t *testing.T) {
	f := newFixture(t)
	withExchange(f)
	interceptor := f.interceptor(t)
	service := newTokenService(t, f, host)
	worker := authenticationtest.NewContext(context.Background(), &authenticationpb.Session{
		Principal: &authenticationpb.Principal{Subject: "serviceAccounts/worker"},
	})
	requireExpiresIn := func(t *testing.T, lifetime time.Duration, response *authenticationpb.ExchangeTokenResponse) {
		t.Helper()
		require.WithinDuration(t, time.Now().Add(lifetime), response.GetExpireTime().AsTime(), 2*time.Second)
		token, err := jwt.ParseInsecure([]byte(response.GetAccessToken()))
		require.NoError(t, err)
		require.Equal(t, response.GetExpireTime().AsTime(), token.Expiration())
	}

	t.Run("the token proves the subject, acted for by the actor, with the grants the issuer computes from the host's claims", func(t *testing.T) {
		response, err := service.ExchangeToken(worker, exchangeRequest(alice))
		require.NoError(t, err)
		seen, err := unary(t, interceptor, getBookMethod, bearer(response.GetAccessToken()), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, alice, seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "agents/1", seen.session.GetActor().GetSubject())
		require.Nil(t, seen.session.GetCaller())
		expected := []*authenticationpb.Grant{{Role: "reader", Scope: "organizations/1"}}
		require.Len(t, seen.session.GetGrants(), len(expected))
		for index, grant := range expected {
			require.True(t, proto.Equal(grant, seen.session.GetGrants()[index]), "%v", seen.session.GetGrants())
		}
	})
	t.Run("a request without a lifetime gets the issuer's", func(t *testing.T) {
		response, err := service.ExchangeToken(worker, exchangeRequest(alice))
		require.NoError(t, err)
		requireExpiresIn(t, 10*time.Minute, response)
	})
	t.Run("a lifetime above the issuer's is capped", func(t *testing.T) {
		request := exchangeRequest(alice)
		request.Lifetime = durationpb.New(time.Hour)
		response, err := service.ExchangeToken(worker, request)
		require.NoError(t, err)
		requireExpiresIn(t, 10*time.Minute, response)
	})
	t.Run("a shorter lifetime is kept", func(t *testing.T) {
		request := exchangeRequest(alice)
		request.Lifetime = durationpb.New(time.Minute)
		response, err := service.ExchangeToken(worker, request)
		require.NoError(t, err)
		requireExpiresIn(t, time.Minute, response)
	})
	t.Run("an unknown issuer", func(t *testing.T) {
		request := exchangeRequest(alice)
		request.Issuer = "ghost"
		_, err := service.ExchangeToken(worker, request)
		require.True(t, status.HasCode(err, codes.InvalidArgument), "%v", err)
	})
	t.Run("an issuer not exchanged for", func(t *testing.T) {
		request := exchangeRequest(alice)
		request.Issuer = "test"
		_, err := service.ExchangeToken(worker, request)
		require.True(t, status.HasCode(err, codes.InvalidArgument), "%v", err)
	})
	t.Run("a subject the host does not know", func(t *testing.T) {
		_, err := service.ExchangeToken(worker, exchangeRequest("organizations/1/users/ghost"))
		require.True(t, status.HasCode(err, codes.NotFound), "%v", err)
	})
	t.Run("a host error refuses with its code", func(t *testing.T) {
		_, err := service.ExchangeToken(worker, exchangeRequest(dan))
		require.True(t, status.HasCode(err, codes.Unavailable), "%v", err)
	})
	t.Run("claims naming one the exchange writes", func(t *testing.T) {
		_, err := service.ExchangeToken(worker, exchangeRequest(eve))
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
		require.ErrorContains(t, err, `"exp"`)
	})
	t.Run("an issuer mapping the token to another actor", func(t *testing.T) {
		request := exchangeRequest(alice)
		request.Issuer = "kenji"
		_, err := service.ExchangeToken(worker, request)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
		require.ErrorContains(t, err, `acted for by ""`)
	})
	t.Run("a call made by an actor", func(t *testing.T) {
		ctx := authenticationtest.NewContext(context.Background(), &authenticationpb.Session{
			Principal: &authenticationpb.Principal{Subject: bob},
			Actor:     &authenticationpb.Principal{Subject: "agents/1"},
		})
		_, err := service.ExchangeToken(ctx, exchangeRequest(alice))
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})
	t.Run("a call no interceptor authenticated", func(t *testing.T) {
		_, err := service.ExchangeToken(context.Background(), exchangeRequest(alice))
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
}

// A service calling as itself exchanges, then calls the platform with the token as any client would: the handler sees
// the subject, acted for by the actor.
func TestExchangeTokenOverThePlatform(t *testing.T) {
	f := newFixture(t)
	withExchange(f)
	p := newPlatform(t, f)
	server := p.newServer()
	authenticationpb.RegisterTokenServiceServer(server, newTokenService(t, f, host))
	socket := filepath.Join(p.directory, "token.socket")
	p.serve(t, server, socket)
	tokenService := authenticationpb.NewTokenServiceClient(p.dial(t, socket))
	library := librarypb.NewLibraryServiceClient(p.dial(t, p.librarySocket))

	t.Run("a service exchanges as itself and calls as the subject", func(t *testing.T) {
		response, err := tokenService.ExchangeToken(p.as(t, "worker"), exchangeRequest(alice))
		require.NoError(t, err)
		ctx := metadata.NewOutgoingContext(context.Background(), bearer(response.GetAccessToken()))
		_, err = library.GetBook(ctx, getBookRequest)
		require.NoError(t, err)
		seen := p.library.last()
		require.Equal(t, alice, seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "agents/1", seen.session.GetActor().GetSubject())
		require.Nil(t, seen.session.GetCaller())
	})
	t.Run("a caller the rule refuses", func(t *testing.T) {
		_, err := tokenService.ExchangeToken(p.as(t, "gateway"), exchangeRequest(alice))
		require.True(t, status.HasCode(err, codes.PermissionDenied), "gateway holds no exchanger: %v", err)

		request := exchangeRequest(alice)
		request.Actor = "users/mallory"
		_, err = tokenService.ExchangeToken(p.as(t, "worker"), request)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "the rule reads the actor: %v", err)
	})
	t.Run("an exchanged token is never exchanged again", func(t *testing.T) {
		response, err := tokenService.ExchangeToken(p.as(t, "worker"), exchangeRequest(bob))
		require.NoError(t, err)
		ctx := metadata.NewOutgoingContext(context.Background(), bearer(response.GetAccessToken()))
		_, err = tokenService.ExchangeToken(ctx, exchangeRequest(alice))
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
		require.ErrorContains(t, err, "never exchanges")
	})
}
