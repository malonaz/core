package authentication_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	aipb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarymodelpb "github.com/malonaz/core/genproto/test/library/v1"
)

const (
	updateBookMethod            = "/" + libraryService + "/UpdateBook"
	deleteBookMethod            = "/" + libraryService + "/DeleteBook"
	searchBooksMethod           = "/" + libraryService + "/SearchBooks"
	undeleteShelfMethod         = "/" + libraryService + "/UndeleteShelf"
	batchGetBooksMethod         = "/" + libraryService + "/BatchGetBooks"
	batchCreateShelvesMethod    = "/" + libraryService + "/BatchCreateShelves"
	importBooksMethod           = "/" + libraryService + "/ImportBooks"
	streamGenerateMessageMethod = "/malonaz.ai.ai_service.v1.AiService/StreamGenerateMessage"
)

// newScopedFixture returns the shared fixture with a librarian role over one method of every shape, and two whose
// resources cannot be derived; the test issuer grants it over organizations/1.
func newScopedFixture(t testing.TB) *fixture {
	t.Helper()
	f := newFixture(t)
	f.configuration.Roles = append(f.configuration.Roles, &authenticationpb.Role{
		Id: "librarian",
		Rules: []*authenticationpb.Rule{{Methods: []string{
			createBookMethod, listBooksMethod, searchBooksMethod, getBookMethod, deleteBookMethod, undeleteShelfMethod,
			updateBookMethod, batchGetBooksMethod, batchCreateShelvesMethod, streamGenerateMessageMethod,
			importBooksMethod, echoStreamMethod, reflectionMethod,
		}}},
	})
	f.issuer.GrantsCel = `[Grant{role: "librarian", scope: "organizations/1"}]`
	return f
}

// A scoped grant allows a call when its scope covers every resource the call names, read from the request by the
// method's shape. The resources are covered segment for segment: a wildcard is covered only by "*".
func TestScopedGrantCoversTheResourcesACallNames(t *testing.T) {
	const (
		book      = "organizations/1/shelves/1/books/1"
		otherBook = "organizations/2/shelves/1/books/1"
	)
	createShelf := func(parent string) *librarypb.CreateShelfRequest {
		return &librarypb.CreateShelfRequest{Parent: parent}
	}
	for name, test := range map[string]struct {
		method  string
		request proto.Message
		allowed bool
	}{
		"create: its parent":                         {createBookMethod, &librarypb.CreateBookRequest{Parent: "organizations/1/shelves/1"}, true},
		"create: a parent outside":                   {createBookMethod, &librarypb.CreateBookRequest{Parent: "organizations/2/shelves/1"}, false},
		"list: its parent":                           {listBooksMethod, &librarypb.ListBooksRequest{Parent: "organizations/1/shelves/1"}, true},
		"list: a sibling sharing the prefix":         {listBooksMethod, &librarypb.ListBooksRequest{Parent: "organizations/12/shelves/1"}, false},
		"list: a wildcard beneath the scope":         {listBooksMethod, &librarypb.ListBooksRequest{Parent: "organizations/1/shelves/-"}, true},
		"list: a wildcard in the scope":              {listBooksMethod, &librarypb.ListBooksRequest{Parent: "organizations/-/shelves/-"}, false},
		"list: no parent":                            {listBooksMethod, &librarypb.ListBooksRequest{}, false},
		"search: its parent":                         {searchBooksMethod, &librarypb.SearchBooksRequest{Parent: "organizations/1/shelves/1"}, true},
		"search: a parent outside":                   {searchBooksMethod, &librarypb.SearchBooksRequest{Parent: "organizations/2/shelves/1"}, false},
		"get: its name":                              {getBookMethod, &librarypb.GetBookRequest{Name: book}, true},
		"get: a name outside":                        {getBookMethod, &librarypb.GetBookRequest{Name: otherBook}, false},
		"get: the scope's own resource":              {getBookMethod, &librarypb.GetBookRequest{Name: "organizations/1"}, true},
		"get: no name":                               {getBookMethod, &librarypb.GetBookRequest{}, false},
		"delete: its name":                           {deleteBookMethod, &librarypb.DeleteBookRequest{Name: book}, true},
		"delete: a name outside":                     {deleteBookMethod, &librarypb.DeleteBookRequest{Name: otherBook}, false},
		"custom method: its name":                    {undeleteShelfMethod, &librarypb.UndeleteShelfRequest{Name: "organizations/1/shelves/1"}, true},
		"custom method: a name outside":              {undeleteShelfMethod, &librarypb.UndeleteShelfRequest{Name: "organizations/2/shelves/1"}, false},
		"update: its payload's name":                 {updateBookMethod, &librarypb.UpdateBookRequest{Book: &librarymodelpb.Book{Name: book}}, true},
		"update: a payload outside":                  {updateBookMethod, &librarypb.UpdateBookRequest{Book: &librarymodelpb.Book{Name: otherBook}}, false},
		"update: no payload":                         {updateBookMethod, &librarypb.UpdateBookRequest{}, false},
		"batch get: parent and names":                {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{Parent: "organizations/1/shelves/1", Names: []string{book}}, true},
		"batch get: names, no parent":                {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{Names: []string{book, "organizations/1/shelves/2/books/2"}}, true},
		"batch get: one name outside of several":     {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{Names: []string{book, otherBook, book}}, false},
		"batch get: a name outside its parent":       {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{Parent: "organizations/1/shelves/1", Names: []string{otherBook}}, false},
		"batch get: a wildcard name":                 {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{Names: []string{"organizations/-/shelves/-/books/1"}}, false},
		"batch get: nothing named":                   {batchGetBooksMethod, &librarypb.BatchGetBooksRequest{}, false},
		"batch create: its parent":                   {batchCreateShelvesMethod, &librarypb.BatchCreateShelvesRequest{Parent: "organizations/1", Requests: []*librarypb.CreateShelfRequest{createShelf("")}}, true},
		"batch create: each request's parent":        {batchCreateShelvesMethod, &librarypb.BatchCreateShelvesRequest{Requests: []*librarypb.CreateShelfRequest{createShelf("organizations/1"), createShelf("organizations/1")}}, true},
		"batch create: one request outside":          {batchCreateShelvesMethod, &librarypb.BatchCreateShelvesRequest{Requests: []*librarypb.CreateShelfRequest{createShelf("organizations/1"), createShelf("organizations/2")}}, false},
		"batch create: a request outside its parent": {batchCreateShelvesMethod, &librarypb.BatchCreateShelvesRequest{Parent: "organizations/1", Requests: []*librarypb.CreateShelfRequest{createShelf("organizations/2")}}, false},
		"batch create: a request with no parent":     {batchCreateShelvesMethod, &librarypb.BatchCreateShelvesRequest{Requests: []*librarypb.CreateShelfRequest{createShelf("organizations/1"), createShelf("")}}, false},
		"a method whose resources are not derived":   {importBooksMethod, &librarypb.ImportBooksRequest{Parent: "organizations/1/shelves/1"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newScopedFixture(t)
			_, err := unary(t, f.interceptor(t), test.method, bearer(f.token(t, nil)), test.request)
			if test.allowed {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.PermissionDenied, code(err), "%v", err)
		})
	}
}

// A server stream's resources are read from its one request; a client stream's never are, as only its first
// message is read.
func TestScopedGrantOnStreams(t *testing.T) {
	f := newScopedFixture(t)
	interceptor := f.interceptor(t)
	handler := func(any, grpc.ServerStream) error { return nil }
	ctx := metadata.NewIncomingContext(context.Background(), bearer(f.token(t, nil)))
	serverStream := &grpc.StreamServerInfo{FullMethod: streamGenerateMessageMethod, IsServerStream: true}
	for parent, want := range map[string]codes.Code{
		"organizations/1/users/1/chats/1": codes.OK,
		"organizations/2/users/1/chats/1": codes.PermissionDenied,
	} {
		stream := &requestStream{ctx: ctx, request: &aipb.GenerateMessageRequest{Parent: parent}}
		require.Equal(t, want, code(interceptor.Stream()(nil, stream, serverStream, handler)), parent)
	}
	for _, info := range []*grpc.StreamServerInfo{
		{FullMethod: echoStreamMethod, IsServerStream: true},
		{FullMethod: reflectionMethod, IsClientStream: true, IsServerStream: true},
	} {
		err := interceptor.Stream()(nil, &requestStream{ctx: ctx, request: &librarypb.GetBookRequest{}}, info, handler)
		require.Equal(t, codes.PermissionDenied, code(err), info.FullMethod)
	}
}

// A scoped grant still needs its rule's condition, and a grant over "*" needs no resource at all.
func TestScopedGrantBesideConditionsAndWildcard(t *testing.T) {
	request := &librarypb.ListBooksRequest{Parent: "organizations/1/shelves/1"}
	t.Run("the condition holds", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "lister", scope: "organizations/1"}]`
		_, err := unary(t, f.interceptor(t), listBooksMethod, bearer(f.token(t, map[string]any{"shelf": request.GetParent()})), request)
		require.NoError(t, err)
	})
	t.Run("the condition does not hold", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "lister", scope: "organizations/1"}]`
		_, err := unary(t, f.interceptor(t), listBooksMethod, bearer(f.token(t, map[string]any{"shelf": "x"})), request)
		require.Equal(t, codes.PermissionDenied, code(err), "%v", err)
	})
	t.Run("a grant over * on a method whose resources are not derived", func(t *testing.T) {
		f := newScopedFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "librarian", scope: "*"}]`
		_, err := unary(t, f.interceptor(t), importBooksMethod, bearer(f.token(t, nil)), &librarypb.ImportBooksRequest{})
		require.NoError(t, err)
	})
	for _, scope := range []string{"organizations/-", ""} {
		t.Run("a scope that covers nothing: "+scope, func(t *testing.T) {
			f := newScopedFixture(t)
			f.issuer.GrantsCel = `[Grant{role: "librarian", scope: "` + scope + `"}]`
			_, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, nil)), &librarypb.GetBookRequest{Name: "organizations/-/shelves/-/books/-"})
			require.Equal(t, codes.PermissionDenied, code(err), "%v", err)
		})
	}
}

// A service account's scoped grant is honoured; one that some method of its role could never be allowed under
// refuses to start.
func TestScopedServiceAccountGrant(t *testing.T) {
	f := newFixture(t)
	f.configuration.ServiceAccounts[0].Grants[0].Scope = "organizations/1"
	interceptor := f.interceptor(t)
	_, err := unary(t, interceptor, getBookMethod, apiKey("key-1"), &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"})
	require.NoError(t, err)
	_, err = unary(t, interceptor, getBookMethod, apiKey("key-1"), &librarypb.GetBookRequest{Name: "organizations/2/shelves/1/books/1"})
	require.Equal(t, codes.PermissionDenied, code(err), "%v", err)

	for name, test := range map[string]struct {
		grant *authenticationpb.Grant
		err   string
	}{
		"a method whose resources are not derived": {&authenticationpb.Grant{Role: "librarian", Scope: "organizations/1"}, `the resources ` + reflectionMethod + ` names cannot be derived`},
		"a scope with a wildcard":                  {&authenticationpb.Grant{Role: "reader", Scope: "organizations/-"}, `scope "organizations/-" of role "reader"`},
	} {
		t.Run(name, func(t *testing.T) {
			f := newScopedFixture(t)
			f.configuration.ServiceAccounts[0].Grants = []*authenticationpb.Grant{test.grant}
			_, err := f.build(t)
			require.ErrorContains(t, err, test.err)
		})
	}
}

// requestStream delivers one request to the interceptor's peek.
type requestStream struct {
	grpc.ServerStream
	ctx     context.Context
	request proto.Message
}

func (s *requestStream) Context() context.Context { return s.ctx }

func (s *requestStream) RecvMsg(message any) error {
	bytes, err := proto.Marshal(s.request)
	if err != nil {
		return err
	}
	return proto.Unmarshal(bytes, message.(proto.Message))
}
