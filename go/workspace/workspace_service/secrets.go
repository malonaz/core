package workspace_service

import (
	"bytes"
	"context"
	"os"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// SetSecrets implements the interface: the secrets stay in memory, never on disk.
func (s *Service) SetSecrets(ctx context.Context, request *pb.SetSecretsRequest) (*pb.SetSecretsResponse, error) {
	envVars := map[string]bool{}
	env := make([]string, 0, len(request.GetSecrets()))
	for _, secret := range request.GetSecrets() {
		if envVars[secret.GetEnvVar()] {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate env var %s", secret.GetEnvVar()).Err()
		}
		envVars[secret.GetEnvVar()] = true
		if bytes.IndexByte(secret.GetValue(), 0) >= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "secret %s holds a NUL byte", secret.GetEnvVar()).Err()
		}
		env = append(env, secret.GetEnvVar()+"="+string(secret.GetValue()))
	}
	s.secretsMutex.Lock()
	defer s.secretsMutex.Unlock()
	s.secretEnv = env
	return &pb.SetSecretsResponse{}, nil
}

// commandEnv returns the env of a command: the server's, then the secrets.
func (s *Service) commandEnv() []string {
	s.secretsMutex.RLock()
	defer s.secretsMutex.RUnlock()
	return append(os.Environ(), s.secretEnv...)
}
