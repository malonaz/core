package authentication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const (
	testSecret        = "session-secret"
	createAgentMethod = "/malonaz.ai.agent.agent_service.v1.AgentService/CreateAgent"
	internalService   = authenticationpb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE
)

func serviceAccountIdentity(serviceAccountID string) *authenticationpb.Identity {
	return &authenticationpb.Identity{Identity: &authenticationpb.Identity_ServiceAccountIdentity{
		ServiceAccountIdentity: &authenticationpb.ServiceAccountIdentity{ServiceAccountId: serviceAccountID, ServiceAccountType: internalService},
	}}
}

func jwtIdentity(t *testing.T, claimToValue map[string]any) *authenticationpb.Identity {
	return &authenticationpb.Identity{Identity: &authenticationpb.Identity_JwtIdentity{
		JwtIdentity: &authenticationpb.JwtIdentity{IssuerId: testJwtIssuer.Id, Claims: newClaims(t, claimToValue)},
	}}
}

// sessionOf returns a session acting as an identity.
func sessionOf(t *testing.T, identity *authenticationpb.Identity) *authenticationpb.Session {
	t.Helper()
	session := &authenticationpb.Session{}
	require.NoError(t, setIdentity(session, identity))
	return session
}

// testPermissionInterceptor builds a permission interceptor over a configuration exercising delegation.
func testPermissionInterceptor(t *testing.T, sessionManager *SessionManager) *PermissionAuthenticationInterceptor {
	t.Helper()
	jwtIssuer := proto.CloneOf(testJwtIssuer)
	jwtIssuer.MethodToAuthorizationCel = map[string]string{
		getBookMethod:  `true`,
		listBookMethod: `actor == "" || actor.startsWith("organizations/acme/")`,
	}
	permissionConfig := &authenticationpb.PermissionConfiguration{
		ServiceAccounts: []*authenticationpb.ServiceAccount{
			{
				Id:          "target",
				Type:        internalService,
				Permissions: []string{getBookMethod},
				Labels:      map[string]string{"tier": "gold"},
			},
			{
				Id:       "harper",
				Type:     internalService,
				ActAsCel: `session.jwt_identity.issuer_id == "test-issuer" && session.jwt_identity.claims["admin"] == true && actor == ""`,
			},
			{Id: "locked", Type: internalService},
		},
		Roles:                      []*authenticationpb.Role{{Id: "test-role"}},
		JwtIssuers:                 []*authenticationpb.JwtIssuer{jwtIssuer},
		DelegationAuthorizationCel: `!actor.endsWith("/restricted")`,
	}
	permissionOpts := &PermissionAuthenticationInterceptorOpts{Config: writeConfig(t, "permissions.json", permissionConfig)}
	permissionInterceptor, err := NewPermissionAuthenticationInterceptor(permissionOpts, sessionManager)
	require.NoError(t, err)
	return permissionInterceptor
}

// receive runs a call made on an outgoing context through the server interceptors, as another service receives
// it, returning the session the handler sees.
func receive(t *testing.T, sessionManager *SessionManager, permissionInterceptor *PermissionAuthenticationInterceptor, outgoingCtx context.Context, fullMethod string, request any) (*authenticationpb.Session, error) {
	t.Helper()
	outgoingMetadata, _ := metadata.FromOutgoingContext(outgoingCtx)
	ctx := metadata.NewIncomingContext(context.Background(), outgoingMetadata)
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	var session *authenticationpb.Session
	handler := func(ctx context.Context, request any) (any, error) {
		var err error
		session, err = GetSession(ctx)
		return nil, err
	}
	_, err := sessionManager.UnaryServerLocalContextInjectorInterceptor()(ctx, request, info, func(ctx context.Context, request any) (any, error) {
		return permissionInterceptor.Unary()(ctx, request, info, handler)
	})
	return session, err
}

// withSession returns a context whose outgoing calls carry a session signed as is.
func withSession(t *testing.T, sessionManager *SessionManager, session *authenticationpb.Session) context.Context {
	t.Helper()
	signedSession, err := sessionManager.sign(session)
	require.NoError(t, err)
	bytes, err := pbutil.MarshalDeterministic(signedSession)
	require.NoError(t, err)
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(metadataKeySignedSession, string(bytes)))
}

func TestDelegate(t *testing.T) {
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: testSecret})
	principal := jwtIdentity(t, map[string]any{"sub": "users/alice"})

	t.Run("signs an unauthorized delegated session carried alone", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "x-internal-service-auth", "agent-service/secret", "authorization", "Bearer token")
		ctx, err := sessionManager.Delegate(ctx, principal, "organizations/acme/users/u/agents/a", time.Minute)
		require.NoError(t, err)

		outgoingMetadata, _ := metadata.FromOutgoingContext(ctx)
		require.Len(t, outgoingMetadata, 1)
		values := outgoingMetadata.Get(metadataKeySignedSession)
		require.Len(t, values, 1)
		signedSession := &authenticationpb.SignedSession{}
		require.NoError(t, pbutil.Unmarshal([]byte(values[0]), signedSession))
		require.NoError(t, sessionManager.verify(signedSession))

		session := signedSession.Session
		require.False(t, session.Authorized)
		require.Equal(t, "organizations/acme/users/u/agents/a", session.GetDelegation().GetActor())
		require.True(t, proto.Equal(principal.GetJwtIdentity(), session.GetJwtIdentity()))
		require.NotEmpty(t, session.Id)
		require.WithinDuration(t, time.Now().Add(time.Minute), session.GetExpireTime().AsTime(), 5*time.Second)
	})

	t.Run("refuses a delegation without an actor, a lifetime or a principal", func(t *testing.T) {
		_, err := sessionManager.Delegate(context.Background(), principal, "", time.Minute)
		require.Error(t, err)
		_, err = sessionManager.Delegate(context.Background(), principal, "agents/a", 0)
		require.Error(t, err)
		_, err = sessionManager.Delegate(context.Background(), &authenticationpb.Identity{}, "agents/a", time.Minute)
		require.Error(t, err)
	})
}

func TestVerify(t *testing.T) {
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: testSecret})
	session := sessionOf(t, serviceAccountIdentity("target"))

	session.ExpireTime = timestamppb.New(time.Now().Add(time.Minute))
	signedSession, err := sessionManager.sign(session)
	require.NoError(t, err)
	require.NoError(t, sessionManager.verify(signedSession))

	signedSession.Session.Authorized = true
	require.True(t, status.HasCode(sessionManager.verify(signedSession), codes.Unauthenticated), "a tampered session is rejected")

	session.Authorized = false
	session.ExpireTime = timestamppb.New(time.Now().Add(-time.Second))
	signedSession, err = sessionManager.sign(session)
	require.NoError(t, err)
	err = sessionManager.verify(signedSession)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	require.ErrorContains(t, err, "expired")
}

func TestPermissionInterceptor_Delegation(t *testing.T) {
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: testSecret})
	permissionInterceptor := testPermissionInterceptor(t, sessionManager)
	alice := jwtIdentity(t, map[string]any{"sub": "users/alice"})
	delegate := func(principal *authenticationpb.Identity, actor string, lifetime time.Duration) context.Context {
		ctx, err := sessionManager.Delegate(context.Background(), principal, actor, lifetime)
		require.NoError(t, err)
		return ctx
	}

	t.Run("issuer CEL reads the actor", func(t *testing.T) {
		session, err := receive(t, sessionManager, permissionInterceptor, delegate(alice, "organizations/acme/users/u/agents/a", time.Minute), listBookMethod, nil)
		require.NoError(t, err)
		require.True(t, session.Authorized, "the target authorizes the session once it checked it")
		require.Equal(t, "organizations/acme/users/u/agents/a", session.GetDelegation().GetActor())
		require.Equal(t, "users/alice", session.GetJwtIdentity().GetClaims().AsMap()["sub"])

		_, err = receive(t, sessionManager, permissionInterceptor, delegate(alice, "organizations/other/users/u/agents/a", time.Minute), listBookMethod, nil)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})

	t.Run("the target re-checks the principal's permissions", func(t *testing.T) {
		_, err := receive(t, sessionManager, permissionInterceptor, delegate(serviceAccountIdentity("target"), "agents/a", time.Minute), getBookMethod, nil)
		require.NoError(t, err)
		_, err = receive(t, sessionManager, permissionInterceptor, delegate(serviceAccountIdentity("target"), "agents/a", time.Minute), listBookMethod, nil)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
	})

	t.Run("delegation CEL restricts delegated sessions only", func(t *testing.T) {
		_, err := receive(t, sessionManager, permissionInterceptor, delegate(serviceAccountIdentity("target"), "agents/restricted", time.Minute), getBookMethod, nil)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
		_, err = receive(t, sessionManager, permissionInterceptor, withSession(t, sessionManager, sessionOf(t, serviceAccountIdentity("target"))), getBookMethod, nil)
		require.NoError(t, err)
	})

	t.Run("a service account principal carries its configured labels", func(t *testing.T) {
		session, err := receive(t, sessionManager, permissionInterceptor, delegate(serviceAccountIdentity("target"), "agents/a", time.Minute), getBookMethod, nil)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"tier": "gold"}, session.Labels)
	})

	t.Run("an expired delegated session is rejected", func(t *testing.T) {
		ctx := delegate(serviceAccountIdentity("target"), "agents/a", time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		_, err := receive(t, sessionManager, permissionInterceptor, ctx, getBookMethod, nil)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
}

func TestPermissionInterceptor_ActAs(t *testing.T) {
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: testSecret})
	permissionInterceptor := testPermissionInterceptor(t, sessionManager)
	admin := sessionOf(t, jwtIdentity(t, map[string]any{"sub": "users/admin", "admin": true}))
	user := sessionOf(t, jwtIdentity(t, map[string]any{"sub": "users/user", "admin": false}))
	delegatedAdmin := proto.CloneOf(admin)
	delegatedAdmin.Delegation = &authenticationpb.Delegation{Actor: "agents/a"}
	locked := sessionOf(t, serviceAccountIdentity("locked"))
	mismatchedType := serviceAccountIdentity("harper")
	mismatchedType.GetServiceAccountIdentity().ServiceAccountType = authenticationpb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY

	for _, test := range []struct {
		name      string
		session   *authenticationpb.Session
		principal *authenticationpb.Identity
		allowed   bool
	}{
		{"act_as_cel admits the caller", admin, serviceAccountIdentity("harper"), true},
		{"act_as_cel refuses the caller", user, serviceAccountIdentity("harper"), false},
		{"act_as_cel refuses an actor", delegatedAdmin, serviceAccountIdentity("harper"), false},
		{"no act_as_cel admits nobody", admin, serviceAccountIdentity("locked"), false},
		{"a service account acts as itself", locked, serviceAccountIdentity("locked"), true},
		{"a JWT identity is never named", admin, jwtIdentity(t, map[string]any{"sub": "users/admin", "admin": true}), false},
		{"an unknown service account", admin, serviceAccountIdentity("unknown"), false},
		{"a mismatched service account type", admin, mismatchedType, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := permissionInterceptor.actAs.authorize(test.session, test.principal)
			if test.allowed {
				require.NoError(t, err)
				return
			}
			require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
		})
	}

	t.Run("the interceptor checks the principal field, even on authorized sessions", func(t *testing.T) {
		createAgentRequest := &agentservicepb.CreateAgentRequest{Agent: &agentpb.Agent{Principal: serviceAccountIdentity("harper")}}
		authorizedUser := proto.CloneOf(user)
		authorizedUser.Authorized = true
		_, err := receive(t, sessionManager, permissionInterceptor, withSession(t, sessionManager, authorizedUser), createAgentMethod, createAgentRequest)
		require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)

		authorizedAdmin := proto.CloneOf(admin)
		authorizedAdmin.Authorized = true
		_, err = receive(t, sessionManager, permissionInterceptor, withSession(t, sessionManager, authorizedAdmin), createAgentMethod, createAgentRequest)
		require.NoError(t, err)

		// No principal named: the caller's own identity, nothing to check.
		_, err = receive(t, sessionManager, permissionInterceptor, withSession(t, sessionManager, authorizedUser), createAgentMethod, &agentservicepb.CreateAgentRequest{Agent: &agentpb.Agent{}})
		require.NoError(t, err)
	})
}

func TestResolvePrincipalPath(t *testing.T) {
	createAgentRequest := (&agentservicepb.CreateAgentRequest{}).ProtoReflect().Descriptor()
	path, err := resolvePrincipalPath(createAgentRequest, "agent.principal")
	require.NoError(t, err)
	require.Len(t, path, 2)
	for _, principalField := range []string{"agent", "agent.title", "agent.missing", "parent.principal"} {
		_, err := resolvePrincipalPath(createAgentRequest, principalField)
		require.Error(t, err, principalField)
	}
}

func TestMintDelegatedJwt(t *testing.T) {
	jwtMinter, err := NewJwtMinter(testJwtIssuer)
	require.NoError(t, err)
	token, err := jwtMinter.MintDelegatedJwt("users/alice", "organizations/acme/users/u/agents/a", time.Minute)
	require.NoError(t, err)
	session, err := authenticate(t, listBookMethod, bearer(token))
	require.NoError(t, err)
	require.Equal(t, "organizations/acme/users/u/agents/a", session.GetDelegation().GetActor())
	require.Equal(t, "users/alice", session.GetJwtIdentity().GetClaims().AsMap()["sub"])

	_, err = jwtMinter.MintDelegatedJwt("users/alice", "", time.Minute)
	require.Error(t, err)
}

func TestSessionPropagation_CarriesOneSession(t *testing.T) {
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: testSecret})
	principal := jwtIdentity(t, map[string]any{"sub": "users/alice"})
	delegatedCtx, err := sessionManager.Delegate(context.Background(), principal, "organizations/acme/users/u/agents/a", time.Minute)
	require.NoError(t, err)
	outgoingMetadata, _ := metadata.FromOutgoingContext(delegatedCtx)
	ctx := metadata.NewIncomingContext(context.Background(), outgoingMetadata)

	ctx, err = sessionManager.injectSignedSessionFromIncomingContextToLocalContext(ctx)
	require.NoError(t, err)
	incomingMetadata, _ := metadata.FromIncomingContext(ctx)
	require.Empty(t, incomingMetadata.Get(metadataKeySignedSession))

	// A proxy forwarding the original header still sends a single session.
	ctx = metadata.NewOutgoingContext(ctx, outgoingMetadata)
	ctx, err = sessionManager.injectSignedSessionFromLocalContextToOutgoingContext(ctx)
	require.NoError(t, err)
	outgoingMetadata, _ = metadata.FromOutgoingContext(ctx)
	require.Len(t, outgoingMetadata.Get(metadataKeySignedSession), 1)
}
