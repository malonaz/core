package authentication_test

import (
	"context"
	"strings"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

const customHeader = "x-custom-proof"

// headerAuthenticator proves its session, one it shares between calls, when the call carries its header.
type headerAuthenticator struct {
	session *authenticationpb.Session
	err     error
}

func (a *headerAuthenticator) Authenticate(ctx context.Context) (*authenticationpb.Session, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get(customHeader)) == 0 {
		return nil, nil
	}
	if a.err != nil {
		return nil, a.err
	}
	return a.session, nil
}

// Rows 4 and 5, and every combination of the credentials a call can carry: a bearer token, the cookie, an API key,
// a server's token, and another authenticator's proof. Exactly one explicit credential is used; the cookie only
// without one; two explicit credentials, or two of one kind, are UNAUTHENTICATED.
func TestEdge_CredentialCombinations(t *testing.T) {
	f := newFixture(t)
	cookieToken, _, err := f.signer.Sign(tokenFor("users/cookie"))
	require.NoError(t, err)
	authenticator := &headerAuthenticator{session: &authenticationpb.Session{
		Principal: &authenticationpb.Principal{Subject: "custom/1"},
		Grants:    []*authenticationpb.Grant{{Role: "reader", Scope: "*"}},
	}}
	interceptor := f.interceptor(t, authenticator)
	carried := withClaims(serverClaims(), map[string]any{
		"sub":     "users/carried",
		"session": encodeSession(&authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/carried"}}),
	})

	type credential struct {
		name     string
		md       metadata.MD
		explicit bool
		subject  string
	}
	credentials := []credential{
		{name: "bearer", md: bearer(f.token(t, nil)), explicit: true, subject: "users/alice"},
		{name: "cookie", md: webCookie(cookieToken), subject: "users/cookie"},
		{name: "api key", md: apiKey("key-1"), explicit: true, subject: "serviceAccounts/onikiri"},
		{name: "server token", md: serverToken(sign(t, serverSecret, jwa.HS256, carried)), explicit: true, subject: "users/carried"},
		{name: "authenticator", md: metadata.Pairs(customHeader, "1"), explicit: true, subject: "custom/1"},
	}
	for subset := range 1 << len(credentials) {
		var names []string
		md := metadata.MD{}
		explicit := 0
		subject := ""
		cookie := ""
		for index, credential := range credentials {
			if subset&(1<<index) == 0 {
				continue
			}
			names = append(names, credential.name)
			md = metadata.Join(md, credential.md)
			if credential.explicit {
				explicit++
				subject = credential.subject
			} else {
				cookie = credential.subject
			}
		}
		if explicit == 0 {
			subject = cookie
		}
		name := strings.Join(names, " + ")
		if name == "" {
			name = "none"
		}
		t.Run(name, func(t *testing.T) {
			seen, err := unary(t, interceptor, getBookMethod, md, getBookRequest)
			switch {
			case explicit > 1:
				require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
			case subject == "":
				require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
				seen, err := unary(t, interceptor, echoMethod, md, nil)
				require.NoError(t, err)
				require.Nil(t, seen.session.GetPrincipal())
			default:
				require.NoError(t, err)
				require.Equal(t, subject, seen.session.GetPrincipal().GetSubject())
			}
		})
	}

	carriedToken := sign(t, serverSecret, jwa.HS256, carried)
	for name, md := range map[string]metadata.MD{
		"two bearer tokens":             metadata.Join(bearer(f.token(t, nil)), bearer(f.token(t, nil))),
		"two api keys":                  metadata.Join(apiKey("key-1"), apiKey("key-2")),
		"two server tokens":             metadata.Join(serverToken(carriedToken), serverToken(carriedToken)),
		"invalid bearer, valid api key": metadata.Join(bearer("not-a-jwt"), apiKey("key-1")),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := unary(t, interceptor, getBookMethod, md, getBookRequest)
			require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
		})
	}

	t.Run("an authorization header of another scheme is no credential, as in v1", func(t *testing.T) {
		seen, err := unary(t, interceptor, getBookMethod, metadata.Join(metadata.Pairs("authorization", "Basic dXNlcjpwYXNz"), apiKey("key-1")), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/onikiri", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, []string{"Basic dXNlcjpwYXNz"}, seen.metadata.Get("authorization"))
	})
	t.Run("every key of an account proves it", func(t *testing.T) {
		for _, key := range []string{"key-1", "key-2"} {
			seen, err := unary(t, interceptor, getBookMethod, apiKey(key), getBookRequest)
			require.NoError(t, err)
			require.Equal(t, "serviceAccounts/onikiri", seen.session.GetPrincipal().GetSubject())
		}
	})
	t.Run("an authenticator's error is returned", func(t *testing.T) {
		failing := &headerAuthenticator{err: status.Errorf(codes.Unauthenticated, "bad proof").Err()}
		_, err := unary(t, f.interceptor(t, failing), echoMethod, metadata.Pairs(customHeader, "1"), nil)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
	t.Run("an authenticator's session is copied: the call's client is written into the copy", func(t *testing.T) {
		for _, ipAddress := range []string{"198.51.100.1", "203.0.113.2"} {
			seen, err := unary(t, interceptor, getBookMethod, metadata.Pairs(customHeader, "1", "x-forwarded-for", ipAddress), getBookRequest)
			require.NoError(t, err)
			require.Equal(t, ipAddress, seen.session.GetClient().GetIpAddress())
		}
		require.Nil(t, authenticator.session.GetClient())
	})
	t.Run("the credential used never reaches the handler, whatever the case of the configured header", func(t *testing.T) {
		f := newFixture(t)
		f.opts.APIKeyHeader = "X-Api-Key"
		interceptor := f.interceptor(t)
		for name, md := range map[string]metadata.MD{
			"bearer":       bearer(f.token(t, nil)),
			"api key":      apiKey("key-1"),
			"server token": serverToken(sign(t, serverSecret, jwa.HS256, carried)),
		} {
			seen, err := unary(t, interceptor, getBookMethod, md, getBookRequest)
			require.NoError(t, err, name)
			for key := range md {
				require.Empty(t, seen.metadata.Get(key), name)
			}
		}
	})
}

// countingStream is a server stream whose client sends one request, counting the handler's and interceptor's reads.
type countingStream struct {
	grpc.ServerStream
	ctx   context.Context
	reads int
}

func (s *countingStream) Context() context.Context { return s.ctx }

func (s *countingStream) RecvMsg(message any) error {
	s.reads++
	return nil
}

// A stream's credential is checked before its first request is read: a bad one is refused with nothing read.
func TestStreamAuthenticatesBeforeReading(t *testing.T) {
	interceptor := newFixture(t).interceptor(t)
	info := &grpc.StreamServerInfo{FullMethod: echoStreamMethod, IsServerStream: true}
	handler := func(any, grpc.ServerStream) error { return nil }
	anonymous := &countingStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.MD{})}
	err := interceptor.Stream()(nil, anonymous, info, handler)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	require.Equal(t, 1, anonymous.reads, "no credential is no bad credential: the request is read for the decision")

	badToken := &countingStream{ctx: metadata.NewIncomingContext(context.Background(), bearer("not-a-jwt"))}
	err = interceptor.Stream()(nil, badToken, info, handler)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	require.Zero(t, badToken.reads)
}
