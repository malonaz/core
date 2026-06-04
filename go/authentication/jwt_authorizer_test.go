package authentication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
)

const (
	getBookMethod  = "/malonaz.test.library.library_service.v1.LibraryService/GetBook"
	listBookMethod = "/malonaz.test.library.library_service.v1.LibraryService/ListBooks"
)

func newTestAuthorizer(t *testing.T, methodToExpression map[string]string) *jwtAuthorizer {
	t.Helper()
	jwtIssuer := &authenticationpb.JwtIssuer{
		Id:                       "test-issuer",
		MethodToAuthorizationCel: methodToExpression,
	}
	authorizer, err := newJwtAuthorizer(jwtIssuer)
	require.NoError(t, err)
	return authorizer
}

func newClaims(t *testing.T, claimToValue map[string]any) *structpb.Struct {
	t.Helper()
	claims, err := structpb.NewStruct(claimToValue)
	require.NoError(t, err)
	return claims
}

func TestJwtAuthorizer_MethodNotConfigured(t *testing.T) {
	authorizer := newTestAuthorizer(t, map[string]string{})
	allowed, err := authorizer.authorize(getBookMethod, newClaims(t, nil), nil)
	require.ErrorIs(t, err, ErrMethodNotConfigured)
	require.False(t, allowed)
}

func TestJwtAuthorizer_ClaimsBased(t *testing.T) {
	authorizer := newTestAuthorizer(t, map[string]string{
		getBookMethod: `claims.role == "admin"`,
	})

	adminClaims := newClaims(t, map[string]any{"role": "admin"})
	allowed, err := authorizer.authorize(getBookMethod, adminClaims, nil)
	require.NoError(t, err)
	require.True(t, allowed)

	userClaims := newClaims(t, map[string]any{"role": "user"})
	allowed, err = authorizer.authorize(getBookMethod, userClaims, nil)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestJwtAuthorizer_RequestBased(t *testing.T) {
	authorizer := newTestAuthorizer(t, map[string]string{
		getBookMethod: `request.name.startsWith(claims.org)`,
	})

	claims := newClaims(t, map[string]any{"org": "organizations/acme"})

	getBookRequest := &librarypb.GetBookRequest{
		Name: "organizations/acme/shelves/fiction/books/dune",
	}
	allowed, err := authorizer.authorize(getBookMethod, claims, getBookRequest)
	require.NoError(t, err)
	require.True(t, allowed)

	getBookRequest = &librarypb.GetBookRequest{
		Name: "organizations/other/shelves/fiction/books/dune",
	}
	allowed, err = authorizer.authorize(getBookMethod, claims, getBookRequest)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestJwtAuthorizer_RequestFieldAccess(t *testing.T) {
	authorizer := newTestAuthorizer(t, map[string]string{
		listBookMethod: `request.parent == claims.allowed_parent`,
	})

	claims := newClaims(t, map[string]any{"allowed_parent": "organizations/acme/shelves/fiction"})

	listBooksRequest := &librarypb.ListBooksRequest{
		Parent: "organizations/acme/shelves/fiction",
	}
	allowed, err := authorizer.authorize(listBookMethod, claims, listBooksRequest)
	require.NoError(t, err)
	require.True(t, allowed)

	listBooksRequest = &librarypb.ListBooksRequest{
		Parent: "organizations/acme/shelves/history",
	}
	allowed, err = authorizer.authorize(listBookMethod, claims, listBooksRequest)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestNewJwtAuthorizer_NonBoolExpression(t *testing.T) {
	jwtIssuer := &authenticationpb.JwtIssuer{
		Id:                       "test-issuer",
		MethodToAuthorizationCel: map[string]string{getBookMethod: `claims.role`},
	}
	_, err := newJwtAuthorizer(jwtIssuer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must return bool")
}

func TestNewJwtAuthorizer_InvalidExpression(t *testing.T) {
	jwtIssuer := &authenticationpb.JwtIssuer{
		Id:                       "test-issuer",
		MethodToAuthorizationCel: map[string]string{getBookMethod: `this is not valid cel ===`},
	}
	_, err := newJwtAuthorizer(jwtIssuer)
	require.Error(t, err)
}
