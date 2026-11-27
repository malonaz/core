package workspace_service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

// readDirectory returns the files of a directory by name, and their content.
func readDirectory(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	nameToContent := map[string]string{}
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		require.NoError(t, err)
		nameToContent[entry.Name()] = string(content)
	}
	return nameToContent
}

func TestSetSecrets(t *testing.T) {
	runDirectory := t.TempDir()
	service, err := New(&Opts{Directory: t.TempDir(), RunDirectory: runDirectory})
	require.NoError(t, err)
	t.Setenv("BASH_ENV", filepath.Join(runDirectory, "env"))

	setSecretsRequest := &pb.SetSecretsRequest{Secrets: []*pb.Secret{
		{Id: "github", EnvVar: "GITHUB_TOKEN", Value: []byte("it's $HOME")},
		{Id: "key-file", Value: []byte("key")},
	}}
	_, err = service.SetSecrets(context.Background(), setSecretsRequest)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"github": "it's $HOME", "key-file": "key"}, readDirectory(t, filepath.Join(runDirectory, "secrets")))

	// Every shell sources the env file, unexpanded.
	execRequest := &pb.ExecRequest{Command: `printf %s "$GITHUB_TOKEN"`}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{Output: "it's $HOME"}, execResponse)

	// A replacement drops the secrets it leaves out.
	setSecretsRequest = &pb.SetSecretsRequest{Secrets: []*pb.Secret{{Id: "key-file", Value: []byte("new")}}}
	_, err = service.SetSecrets(context.Background(), setSecretsRequest)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"key-file": "new"}, readDirectory(t, filepath.Join(runDirectory, "secrets")))
	execResponse, err = service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{}, execResponse)
}

func TestSetSecretsInvalid(t *testing.T) {
	service, err := New(&Opts{Directory: t.TempDir(), RunDirectory: t.TempDir()})
	require.NoError(t, err)
	for name, secrets := range map[string][]*pb.Secret{
		"duplicate id":      {{Id: "a"}, {Id: "a"}},
		"duplicate env var": {{Id: "a", EnvVar: "A"}, {Id: "b", EnvVar: "A"}},
		"NUL in env var":    {{Id: "a", EnvVar: "A", Value: []byte{'x', 0}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := service.SetSecrets(context.Background(), &pb.SetSecretsRequest{Secrets: secrets})
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}
}
