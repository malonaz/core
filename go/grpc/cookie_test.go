package grpc

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
)

func TestCookieFromProto(t *testing.T) {
	testCases := []struct {
		name       string
		httpCookie *grpcpb.HttpCookie
		expected   string
	}{
		{
			name: "unset expires and same site are omitted",
			httpCookie: &grpcpb.HttpCookie{
				Name: "session", Value: "jwt", Path: "/", MaxAge: 600, HttpOnly: true, Secure: true,
			},
			expected: "session=jwt; Path=/; Max-Age=600; HttpOnly; Secure",
		},
		{
			name: "same site lax",
			httpCookie: &grpcpb.HttpCookie{
				Name: "session", Value: "jwt", Path: "/", MaxAge: 600, HttpOnly: true, Secure: true,
				SameSite: grpcpb.SameSite_SAME_SITE_LAX,
			},
			expected: "session=jwt; Path=/; Max-Age=600; HttpOnly; Secure; SameSite=Lax",
		},
		{
			name: "same site strict",
			httpCookie: &grpcpb.HttpCookie{
				Name: "session", Value: "jwt", SameSite: grpcpb.SameSite_SAME_SITE_STRICT,
			},
			expected: "session=jwt; SameSite=Strict",
		},
		{
			name: "same site none",
			httpCookie: &grpcpb.HttpCookie{
				Name: "session", Value: "jwt", Secure: true, SameSite: grpcpb.SameSite_SAME_SITE_NONE,
			},
			expected: "session=jwt; Secure; SameSite=None",
		},
		{
			name: "set expires is sent",
			httpCookie: &grpcpb.HttpCookie{
				Name: "session", Value: "jwt",
				Expires: uint64(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro()),
			},
			expected: "session=jwt; Expires=Wed, 02 Jan 2030 03:04:05 GMT",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cookieFromProto(testCase.httpCookie).String())
		})
	}
}

func TestCookieToProto(t *testing.T) {
	t.Run("unset expires stays zero", func(t *testing.T) {
		// Request cookies carry no expiry: the zero time must not wrap around.
		httpCookie := cookieToProto(&http.Cookie{Name: "session", Value: "jwt"})
		require.Zero(t, httpCookie.GetExpires())
		require.Equal(t, grpcpb.SameSite_SAME_SITE_UNSPECIFIED, httpCookie.GetSameSite())
	})

	t.Run("round trip", func(t *testing.T) {
		for _, sameSite := range []grpcpb.SameSite{
			grpcpb.SameSite_SAME_SITE_UNSPECIFIED,
			grpcpb.SameSite_SAME_SITE_LAX,
			grpcpb.SameSite_SAME_SITE_STRICT,
			grpcpb.SameSite_SAME_SITE_NONE,
		} {
			httpCookie := &grpcpb.HttpCookie{
				Name: "session", Value: "jwt", Path: "/", Domain: "example.com",
				Expires:  uint64(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro()),
				MaxAge:   600,
				HttpOnly: true,
				Secure:   true,
				SameSite: sameSite,
			}
			require.True(t, proto.Equal(httpCookie, cookieToProto(cookieFromProto(httpCookie))), sameSite.String())
		}
	})
}

func TestWebCookieSetHTTPCookies(t *testing.T) {
	serverTransportStream := &fakeServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), serverTransportStream)
	httpCookie := &grpcpb.HttpCookie{
		Name: "session", Value: "jwt", Path: "/", MaxAge: 600, HttpOnly: true, Secure: true,
		SameSite: grpcpb.SameSite_SAME_SITE_LAX,
	}
	require.NoError(t, WebCookie{}.SetHTTPCookies(ctx, httpCookie))
	require.Equal(t,
		[]string{"session=jwt; Path=/; Max-Age=600; HttpOnly; Secure; SameSite=Lax"},
		serverTransportStream.header.Get(grpcWebSetCookieMetadataKey),
	)
}

// fakeServerTransportStream records the headers a handler sends.
type fakeServerTransportStream struct {
	header metadata.MD
}

func (s *fakeServerTransportStream) Method() string { return "/test.Service/Method" }

func (s *fakeServerTransportStream) SetHeader(md metadata.MD) error {
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *fakeServerTransportStream) SendHeader(md metadata.MD) error { return s.SetHeader(md) }

func (s *fakeServerTransportStream) SetTrailer(metadata.MD) error { return nil }
