package middleware

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeClientStream delivers its messages, then ends with err.
type fakeClientStream struct {
	grpc.ClientStream
	messages int
	err      error
}

func (s *fakeClientStream) SendMsg(any) error { return nil }
func (s *fakeClientStream) CloseSend() error  { return nil }

func (s *fakeClientStream) RecvMsg(any) error {
	if s.messages > 0 {
		s.messages--
		return nil
	}
	return s.err
}

func TestStreamClientRetry(t *testing.T) {
	serverStream := &grpc.StreamDesc{ServerStreams: true}
	unavailable := status.Error(codes.Unavailable, "unavailable")
	for _, tc := range []struct {
		name         string
		attempts     []*fakeClientStream
		wantErr      error
		wantMessages int
		wantAttempts int
	}{
		{
			name:         "retries unavailable before the first message",
			attempts:     []*fakeClientStream{{err: unavailable}, {messages: 1, err: io.EOF}},
			wantErr:      io.EOF,
			wantMessages: 1,
			wantAttempts: 2,
		},
		{
			name:         "does not retry unavailable after a message",
			attempts:     []*fakeClientStream{{messages: 1, err: unavailable}, {messages: 1, err: io.EOF}},
			wantErr:      unavailable,
			wantMessages: 1,
			wantAttempts: 1,
		},
		{
			name:         "does not retry other codes",
			attempts:     []*fakeClientStream{{err: status.Error(codes.Internal, "internal")}, {err: io.EOF}},
			wantErr:      status.Error(codes.Internal, "internal"),
			wantAttempts: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			streamer := func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
				attempt := tc.attempts[attempts]
				attempts++
				return attempt, nil
			}
			stream, err := StreamClientRetry()(context.Background(), serverStream, nil, "/test.Service/Stream", streamer)
			require.NoError(t, err)
			require.NoError(t, stream.SendMsg(nil))
			require.NoError(t, stream.CloseSend())
			messages := 0
			for {
				if err = stream.RecvMsg(nil); err != nil {
					break
				}
				messages++
			}
			require.Equal(t, tc.wantErr.Error(), err.Error())
			require.Equal(t, tc.wantMessages, messages)
			require.Equal(t, tc.wantAttempts, attempts)
		})
	}
}
