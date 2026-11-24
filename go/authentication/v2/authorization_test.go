package authentication_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
)

// newAuthorizationService returns the AuthorizationService of a server serving the library service alone, under the
// fixture's roles and these: staff deletes books when the principal is staff, a condition reading only the session.
func newAuthorizationService(t *testing.T) *authentication.AuthorizationService {
	t.Helper()
	fixture := newFixture(t)
	fixture.configuration.Roles = append(fixture.configuration.Roles,
		&authenticationpb.Role{Id: "staff", Rules: []*authenticationpb.Rule{{Methods: []string{deleteBookMethod}, ConditionCel: `principal.attributes.staff == true`}}},
		&authenticationpb.Role{Id: "echoer", Rules: []*authenticationpb.Rule{{Methods: []string{echoMethod}}}},
	)
	server := grpc.NewServer()
	librarypb.RegisterLibraryServiceServer(server, librarypb.UnimplementedLibraryServiceServer{})
	return fixture.interceptor(t).NewAuthorizationService(server)
}

// testMethods asks the service which methods a session could call.
func testMethods(t *testing.T, service *authentication.AuthorizationService, session *authenticationpb.Session, methods ...string) []string {
	t.Helper()
	response, err := service.TestMethods(authenticationtest.NewContext(context.Background(), session), &authenticationpb.TestMethodsRequest{Methods: methods})
	require.NoError(t, err)
	return response.GetMethods()
}

func grants(roleToScope map[string]string) []*authenticationpb.Grant {
	var grants []*authenticationpb.Grant
	for role, scope := range roleToScope {
		grants = append(grants, &authenticationpb.Grant{Role: role, Scope: scope})
	}
	return grants
}

func TestTestMethods_ReturnsTheMethodsAGrantListsInTheOrderAsked(t *testing.T) {
	service := newAuthorizationService(t)
	session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}, Grants: grants(map[string]string{"reader": "*"})}

	methods := testMethods(t, service, session, createBookMethod, getBookMethod, "/no.such.Service/Method", "not a method")

	require.Equal(t, []string{getBookMethod}, methods)
}

func TestTestMethods_ConditionsReadingTheRequestAreReachable(t *testing.T) {
	service := newAuthorizationService(t)
	session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}, Grants: grants(map[string]string{"lister": "*"})}

	require.Equal(t, []string{listBooksMethod}, testMethods(t, service, session, listBooksMethod))
}

func TestTestMethods_ConditionsReadingOnlyTheSessionAreDecided(t *testing.T) {
	service := newAuthorizationService(t)
	for staff, want := range map[bool][]string{true: {deleteBookMethod}, false: nil} {
		attributes, err := structpb.NewStruct(map[string]any{"staff": staff})
		require.NoError(t, err)
		session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice", Attributes: attributes}, Grants: grants(map[string]string{"staff": "*"})}

		require.Equal(t, want, testMethods(t, service, session, deleteBookMethod), "staff: %v", staff)
	}
}

func TestTestMethods_ScopedGrantsReachOnlyWhatAScopeCanCover(t *testing.T) {
	service := newAuthorizationService(t)
	for scope, want := range map[string][]string{"shelves/1": {getBookMethod}, "shelves/-": nil} {
		session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}, Grants: grants(map[string]string{"reader": scope})}

		require.Equal(t, want, testMethods(t, service, session, getBookMethod), "scope %s", scope)
	}
}

func TestTestMethods_AnonymousSessionsReachThePublicRoles(t *testing.T) {
	fixture := newFixture(t)
	server := grpc.NewServer()
	mockpb.RegisterMockServiceServer(server, mockpb.UnimplementedMockServiceServer{})
	librarypb.RegisterLibraryServiceServer(server, librarypb.UnimplementedLibraryServiceServer{})
	service := fixture.interceptor(t).NewAuthorizationService(server)

	require.Equal(t, []string{echoMethod}, testMethods(t, service, &authenticationpb.Session{}, echoMethod, getBookMethod))
}

func TestTestMethods_MethodsTheServerDoesNotServeAreNeverReturned(t *testing.T) {
	service := newAuthorizationService(t)
	session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}, Grants: grants(map[string]string{"echoer": "*", "reader": "*"})}

	require.Equal(t, []string{getBookMethod}, testMethods(t, service, session, echoMethod, getBookMethod))
}
