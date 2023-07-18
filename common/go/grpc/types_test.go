package types_test

import (
	"testing"

	"github.com/bufbuild/protovalidate-go"
	"github.com/stretchr/testify/require"

	typespb "common/go/grpc/types"
)

func TestValidation(t *testing.T) {
	httpCookie := &typespb.HttpCookie{}
	_ = httpCookie

	validator, err := protovalidate.New()
	require.NoError(t, err)

	err = validator.Validate(httpCookie)
	require.Error(t, err)
}
