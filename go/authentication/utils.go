package authentication

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
)

var (
	validator = func() protovalidate.Validator {
		validator, err := protovalidate.New()
		if err != nil {
			panic(fmt.Errorf("instantiating proto validator: %w", err))
		}
		return validator
	}()
)

func parseConfig(path string, config proto.Message) error {
	bytes, err := jsonnet.EvaluateFile(path)
	if err != nil {
		return fmt.Errorf("failed to evaluate config file %s: %w", path, err)
	}
	if err := pbutil.JSONUnmarshalStrict(bytes, config); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	if err := validator.Validate(config); err != nil {
		return fmt.Errorf("validating config: %v", err)
	}
	return nil
}

func extractSessionMetadataFromContext(ctx context.Context) (*authenticationpb.SessionMetadata, error) {
	sessionMetadata := &authenticationpb.SessionMetadata{}

	if p, ok := peer.FromContext(ctx); ok {
		sessionMetadata.IpAddress = p.Addr.String()
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return sessionMetadata, nil
	}

	if values := md.Get("user-agent"); len(values) > 0 {
		sessionMetadata.UserAgent = values[0]
	}

	if values := md.Get(metadataKeyClientPlatform); len(values) > 0 {
		sessionMetadata.ClientPlatform = values[0]
	}

	if values := md.Get(metadataKeyClientVersion); len(values) > 0 {
		clientVersion, err := parseClientVersion(values[0])
		if err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, "parsing client version: %v", err).Err()
		}
		sessionMetadata.ClientVersion = clientVersion
	}

	return sessionMetadata, nil
}

func parseClientVersion(raw string) (*authenticationpb.ClientVersion, error) {
	parts := strings.SplitN(raw, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected format major.minor.patch, got %q", raw)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid major version %q: %v", parts[0], err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version %q: %v", parts[1], err)
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid patch version %q: %v", parts[2], err)
	}
	return &authenticationpb.ClientVersion{
		Major: int32(major),
		Minor: int32(minor),
		Patch: int32(patch),
	}, nil
}
