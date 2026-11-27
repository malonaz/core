package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpoint(t *testing.T) {
	opts := &Opts{Host: "db", Port: 5432, User: "user", Password: "secret", Database: "user", SSLMode: "require"}
	require.Equal(t, "host=db port=5432 user=user dbname=user password=secret sslmode=require", opts.Endpoint())

	opts.IAM = true
	require.Equal(t, "host=db port=5432 user=user dbname=user sslmode=require", opts.Endpoint())
}

func TestStartIAMRequiresSSL(t *testing.T) {
	opts := &Opts{Host: "db", Port: 5432, User: "user", Database: "user", SSLMode: "disable", MaxConns: 1, IAM: true}
	err := NewClient(opts).Start(context.Background())
	require.EqualError(t, err, "iam auth requires ssl")
}
