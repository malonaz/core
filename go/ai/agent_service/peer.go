package agent_service

import (
	"context"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
)

// serviceAccount is the subject of the account the service runs as, which onyx names after the service. Replicas call
// each other as it, carrying the session of the call they forward.
const serviceAccount = "serviceAccounts/agent-service"

// peerClient returns an AgentService client on the replica advertised at address.
func (s *Service) peerClient(ctx context.Context, address string) (agentservicepb.AgentServiceClient, error) {
	connection, err := s.connections.get(ctx, address)
	if err != nil {
		return nil, err
	}
	return agentservicepb.NewAgentServiceClient(connection.Get()), nil
}

// isForwarded reports whether another replica forwarded the call: the receiver acts on its own turn only, never
// forwarding it again.
func isForwarded(ctx context.Context) bool {
	session, err := authentication.GetSession(ctx)
	return err == nil && session.GetCaller().GetSubject() == serviceAccount
}
