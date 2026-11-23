package authentication_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
)

// A condition reads the principal's attributes as JSON gives them (numbers are doubles, a missing key an error), the
// request and the rn.* functions, and may bind names around a rule text it carries unchanged.
func TestRuleConditions(t *testing.T) {
	claims := map[string]any{
		"role": "admin", "count": 3, "admin": true, "null_value": nil, "email": "Alice@Example.com",
		"roles": []any{"admin", "user"}, "nested": map[string]any{"organization": "organizations/1"},
	}
	request := &librarypb.ListBooksRequest{Parent: "organizations/1/shelves/2"}
	for _, test := range []struct {
		condition string
		want      codes.Code
	}{
		{`principal.attributes.role == "admin"`, codes.OK},
		{`principal.attributes["role"] == "admin"`, codes.OK},
		{`has(principal.attributes.role)`, codes.OK},
		{`has(principal.attributes.missing)`, codes.PermissionDenied},
		{`principal.attributes.missing == "x"`, codes.Internal},
		{`type(principal.attributes.count) == double && principal.attributes.count == 3`, codes.OK},
		{`principal.attributes.admin && principal.attributes.null_value == null`, codes.OK},
		{`"admin" in principal.attributes.roles && principal.attributes.roles.exists(r, r == "user")`, codes.OK},
		{`principal.attributes.roles[5] == "admin"`, codes.Internal},
		{`principal.attributes.nested.organization == "organizations/1"`, codes.OK},
		{`principal.attributes.email.lowerAscii() == "alice@example.com"`, codes.OK},
		{`principal.attributes.iat <= principal.attributes.exp && principal.attributes.aud[0] == "test"`, codes.OK},
		{`rn.hasParent(request.parent, principal.attributes.nested.organization)`, codes.OK},
		{`rn.ancestor(request.parent, "organizations/{organization}") == "organizations/12"`, codes.PermissionDenied},
		{`rn.ancestor(principal.subject, "users/{user}") == "users/alice"`, codes.OK},
		{`cel.bind(claims, principal.attributes, cel.bind(subject, principal.subject, (claims.role == "admin" && subject == "users/alice")))`, codes.OK},
	} {
		t.Run(test.condition, func(t *testing.T) {
			f := newFixture(t)
			f.configuration.Roles[2].Rules[0].ConditionCel = test.condition
			_, err := unary(t, f.interceptor(t), listBooksMethod, bearer(f.token(t, claims)), request)
			require.Equal(t, test.want, code(err), "%v", err)
		})
	}
}

// A method pattern's "*" matches any run of characters, dots and slashes included.
func TestRuleMethodPatterns(t *testing.T) {
	methods := []string{getBookMethod, listBooksMethod, createBookMethod, echoStreamMethod, reflectionMethod}
	for _, test := range []struct {
		pattern string
		want    []string
	}{
		{"*", methods},
		{"/*", methods},
		{getBookMethod, []string{getBookMethod}},
		{"/" + libraryService + "/*", []string{getBookMethod, listBooksMethod, createBookMethod}},
		{"/malonaz.test.*", []string{getBookMethod, listBooksMethod, createBookMethod, echoStreamMethod}},
		{"*/Get*", []string{getBookMethod}},
		{"*Book*", []string{getBookMethod, listBooksMethod, createBookMethod}},
		{"/" + mockService + "/Echo*", []string{echoStreamMethod}},
		{"/grpc.*/*Info", []string{reflectionMethod}},
		{"/malonaz.*.v1.*Service/List*", []string{listBooksMethod}},
	} {
		t.Run(test.pattern, func(t *testing.T) {
			f := newFixture(t)
			f.configuration.Roles[1].Rules[0].Methods = []string{test.pattern}
			interceptor := f.interceptor(t)
			var allowed []string
			for _, method := range methods {
				if _, err := unary(t, interceptor, method, apiKey("key-1"), nil); err == nil {
					allowed = append(allowed, method)
				}
			}
			require.Equal(t, test.want, allowed)
		})
	}
}
