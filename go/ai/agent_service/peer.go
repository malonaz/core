package agent_service

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
)

// forwardedMetadataKey marks a call one replica forwards to the replica running the agent's turn: the receiver
// acts on its own turn only, never forwarding it again.
const forwardedMetadataKey = "x-agent-service-forwarded"

// peerClient returns an AgentService client on the replica advertised at address.
func (s *Service) peerClient(ctx context.Context, address string) (agentservicepb.AgentServiceClient, error) {
	connection, err := s.connections.get(ctx, address)
	if err != nil {
		return nil, err
	}
	return agentservicepb.NewAgentServiceClient(connection.Get()), nil
}

// forwardedContext carries an incoming call's metadata, its credentials included, onto a call forwarded to another
// replica, marked as forwarded.
func forwardedContext(ctx context.Context) context.Context {
	outgoingMetadata := metadata.MD{}
	incomingMetadata, _ := metadata.FromIncomingContext(ctx)
	for key, values := range incomingMetadata {
		// Transport headers belong to the incoming call.
		if strings.HasPrefix(key, ":") || strings.HasPrefix(key, "grpc-") || key == "content-type" || key == "user-agent" {
			continue
		}
		outgoingMetadata[key] = values
	}
	outgoingMetadata.Set(forwardedMetadataKey, "true")
	return metadata.NewOutgoingContext(ctx, outgoingMetadata)
}

// isForwarded reports whether an incoming call was forwarded by another replica.
func isForwarded(ctx context.Context) bool {
	return len(metadata.ValueFromIncomingContext(ctx, forwardedMetadataKey)) > 0
}
