package workspace_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestSetSecrets(t *testing.T) {
	service, err := New(&Opts{Directory: t.TempDir()})
	require.NoError(t, err)

	setSecretsRequest := &pb.SetSecretsRequest{Secrets: []*pb.Secret{
		{EnvVar: "GH_TOKEN", Value: []byte("it's $HOME")},
		{EnvVar: "OTHER", Value: []byte("other")},
	}}
	_, err = service.SetSecrets(context.Background(), setSecretsRequest)
	require.NoError(t, err)

	// Every command gets them as env vars, unexpanded.
	execRequest := &pb.ExecRequest{Command: `printf '%s|%s' "$GH_TOKEN" "$OTHER"`}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{Output: "it's $HOME|other"}, execResponse)

	// A replacement drops the secrets it leaves out.
	setSecretsRequest = &pb.SetSecretsRequest{Secrets: []*pb.Secret{{EnvVar: "OTHER", Value: []byte("new")}}}
	_, err = service.SetSecrets(context.Background(), setSecretsRequest)
	require.NoError(t, err)
	execResponse, err = service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{Output: "|new"}, execResponse)
}

func TestSetSecretsInvalid(t *testing.T) {
	service, err := New(&Opts{Directory: t.TempDir()})
	require.NoError(t, err)
	for name, secrets := range map[string][]*pb.Secret{
		"duplicate env var": {{EnvVar: "A"}, {EnvVar: "A"}},
		"NUL in value":      {{EnvVar: "A", Value: []byte{'x', 0}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := service.SetSecrets(context.Background(), &pb.SetSecretsRequest{Secrets: secrets})
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}
}
