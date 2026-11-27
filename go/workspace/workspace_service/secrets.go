package workspace_service

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	secretsDirectory = "secrets"
	envFile          = "env"
)

// SetSecrets implements the interface.
func (s *Service) SetSecrets(ctx context.Context, request *pb.SetSecretsRequest) (*pb.SetSecretsResponse, error) {
	idToValue := map[string][]byte{}
	envVars := map[string]bool{}
	var env strings.Builder
	for _, secret := range request.GetSecrets() {
		if _, ok := idToValue[secret.GetId()]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate secret id %s", secret.GetId()).Err()
		}
		idToValue[secret.GetId()] = secret.GetValue()
		if secret.GetEnvVar() == "" {
			continue
		}
		if envVars[secret.GetEnvVar()] {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate env var %s", secret.GetEnvVar()).Err()
		}
		envVars[secret.GetEnvVar()] = true
		if bytes.IndexByte(secret.GetValue(), 0) >= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "secret %s holds a NUL byte: an env var cannot", secret.GetId()).Err()
		}
		fmt.Fprintf(&env, "export %s=%s\n", secret.GetEnvVar(), shellQuote(string(secret.GetValue())))
	}

	s.secretsMutex.Lock()
	defer s.secretsMutex.Unlock()
	directory := filepath.Join(s.runDirectory, secretsDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fileError(err)
	}
	for id, value := range idToValue {
		if err := writeFileAtomic(filepath.Join(directory, id), value); err != nil {
			return nil, fileError(err)
		}
	}
	if err := writeFileAtomic(filepath.Join(s.runDirectory, envFile), []byte(env.String())); err != nil {
		return nil, fileError(err)
	}
	// Remove the secrets dropped, once the env file no longer names them.
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fileError(err)
	}
	for _, entry := range entries {
		if _, ok := idToValue[entry.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
			return nil, fileError(err)
		}
	}
	return &pb.SetSecretsResponse{}, nil
}

// shellQuote quotes a value for bash: a shell reading it never expands it.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// writeFileAtomic replaces a file whole: a shell reading it sees the old content or the new.
func writeFileAtomic(path string, content []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(content); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
