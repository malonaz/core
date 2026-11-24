package agent_service

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
)

// ownerTokenRenewal is how long before it expires the owner's token is exchanged again, so no call starts on one
// about to lapse.
const ownerTokenRenewal = time.Minute

// ownerToken is the token a turn's tool calls carry: exchanged for the agent's owner, the agent its actor, so the
// owner's own grants decide each call.
type ownerToken struct {
	mutex       sync.Mutex
	accessToken string
	expireTime  time.Time
}

// asOwner returns ctx making calls as the agent's owner, their token its one credential.
func (b *toolbox) asOwner(ctx context.Context) (context.Context, error) {
	b.ownerToken.mutex.Lock()
	defer b.ownerToken.mutex.Unlock()
	if time.Until(b.ownerToken.expireTime) < ownerTokenRenewal {
		agentRn, err := agentpb.ParseAgentRn(b.agent.GetName())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
		}
		exchangeTokenRequest := &authenticationpb.ExchangeTokenRequest{
			Issuer:  b.service.opts.OwnerTokenIssuer,
			Subject: agentRn.UserRn().String(),
			Actor:   b.agent.GetName(),
		}
		exchangeTokenResponse, err := b.service.tokenServiceClient.ExchangeToken(b.service.asService(ctx), exchangeTokenRequest)
		if err != nil {
			return nil, status.FromError(err, "exchanging a token for %s", exchangeTokenRequest.GetSubject()).Err()
		}
		b.ownerToken.accessToken = exchangeTokenResponse.GetAccessToken()
		b.ownerToken.expireTime = exchangeTokenResponse.GetExpireTime().AsTime()
	}
	authorization := map[string]string{"authorization": "Bearer " + b.ownerToken.accessToken}
	return grpc.WithCallMetadata(withoutIdentity(ctx), func(context.Context) (map[string]string, error) { return authorization, nil }), nil
}
