package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/aip"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

// requester is who makes a call: the subject of its principal, and of the actor making it for them, if any. An agent
// sending a message is its own principal.
type requester struct {
	principal string
	actor     string
}

// requesterOf returns who makes the call ctx serves. A call without a principal is refused: it would be recorded as
// no one.
func requesterOf(ctx context.Context) (requester, error) {
	session, err := authentication.GetSession(ctx)
	if err != nil || session.GetPrincipal().GetSubject() == "" {
		return requester{}, status.Errorf(codes.Unauthenticated, "only an authenticated principal can make this call").Err()
	}
	return requester{principal: session.GetPrincipal().GetSubject(), actor: session.GetActor().GetSubject()}, nil
}

// stamp names the requester on a message or a tool result under an annotation, and its actor beside it.
func (r requester) stamp(annotatable aip.Annotatable, annotation aip.StringAnnotation) {
	annotation.Set(annotatable, r.principal)
	if r.actor == "" {
		agentpb.Annotations.Actor.Delete(annotatable)
		return
	}
	agentpb.Annotations.Actor.Set(annotatable, r.actor)
}

// senderOf returns who sent a queued message.
func senderOf(message aip.Annotatable) requester {
	principal, _ := agentpb.Annotations.Sender.Get(message)
	actor, _ := agentpb.Annotations.Actor.Get(message)
	return requester{principal: principal, actor: actor}
}

// resolverOf returns who settled an awaited call.
func resolverOf(resolution *agentpb.AwaitedToolCall_Resolution) requester {
	return requester{principal: resolution.GetResolver(), actor: resolution.GetActor()}
}
