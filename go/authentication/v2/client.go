package authentication

import (
	"cmp"
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	metadataKeyForwardedFor       = "x-forwarded-for"
	metadataKeyGatewayUserAgent   = "grpcgateway-user-agent"
	metadataKeyForwardedUserAgent = "x-forwarded-user-agent"
	metadataKeyUserAgent          = "user-agent"
	metadataKeyClientPlatform     = "x-client-platform"
	metadataKeyClientVersion      = "x-client-version"
	metadataKeyClientTimeZone     = "x-client-timezone"
)

// newClient describes the client a call came from, read from the same headers v1 read.
func newClient(ctx context.Context, md metadata.MD) (*authenticationpb.Client, error) {
	client := &authenticationpb.Client{IpAddress: clientIPAddress(ctx, md)}
	if values := md.Get(metadataKeyGatewayUserAgent); len(values) > 0 {
		client.UserAgent = values[0]
	} else if values := md.Get(metadataKeyForwardedUserAgent); len(values) > 0 {
		client.UserAgent = values[0]
	} else if values := md.Get(metadataKeyUserAgent); len(values) > 0 {
		client.UserAgent = values[0]
	}
	if values := md.Get(metadataKeyClientPlatform); len(values) > 0 {
		client.Platform = values[0]
	}
	if values := md.Get(metadataKeyClientTimeZone); len(values) > 0 {
		client.TimeZone = values[0]
	}
	if values := md.Get(metadataKeyClientVersion); len(values) > 0 {
		version, err := parseClientVersion(values[0])
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "parsing client version: %v", err).Err()
		}
		client.Version = version
	}
	return client, nil
}

// clientIPAddress is X-Forwarded-For's leftmost entry, which the caller controls, as v1 read it; else the peer's.
func clientIPAddress(ctx context.Context, md metadata.MD) string {
	if values := md.Get(metadataKeyForwardedFor); len(values) > 0 {
		return strings.TrimSpace(strings.SplitN(values[0], ",", 2)[0])
	}
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return ""
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
	return &authenticationpb.ClientVersion{Major: int32(major), Minor: int32(minor), Patch: int32(patch)}, nil
}

// CompareClientVersion compares the session's client version with major.minor.patch: -1, 0 or 1 when it is lower,
// equal or higher, and -1 when the session has none.
func CompareClientVersion(session *authenticationpb.Session, major, minor, patch int32) int {
	version := session.GetClient().GetVersion()
	if version == nil {
		return -1
	}
	if version.Major != major {
		return cmp.Compare(version.Major, major)
	}
	if version.Minor != minor {
		return cmp.Compare(version.Minor, minor)
	}
	return cmp.Compare(version.Patch, patch)
}
