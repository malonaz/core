package authentication

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

const actorClaim = "act"

// issuer verifies one issuer's tokens and maps their claims to a session.
type issuer struct {
	config     *authenticationpb.Issuer
	keys       *keySet
	subject    cel.Program
	attributes cel.Program // nil: the claims themselves
	actor      cel.Program
	grants     cel.Program
}

func newIssuer(ctx context.Context, config *authenticationpb.Issuer, claimsEnvironment, grantsEnvironment *cel.Env) (*issuer, error) {
	issuer := &issuer{config: config}
	var err error
	switch keys := config.GetKeys().(type) {
	case *authenticationpb.Issuer_Jwks:
		issuer.keys, err = newInlineKeySet(keys.Jwks)
	case *authenticationpb.Issuer_JwksUri:
		issuer.keys, err = newRemoteKeySet(ctx, keys.JwksUri)
	}
	if err != nil {
		return nil, fmt.Errorf("issuer %q: %w", config.GetId(), err)
	}
	subject := config.GetSubjectCel()
	if subject == "" {
		subject = defaultSubjectExpression
	}
	if issuer.subject, err = compileMapping(claimsEnvironment, subject, types.StringKind); err != nil {
		return nil, fmt.Errorf("issuer %q: subject_cel: %w", config.GetId(), err)
	}
	if attributes := config.GetAttributesCel(); attributes != "" {
		if issuer.attributes, err = compileMapping(claimsEnvironment, attributes, types.MapKind); err != nil {
			return nil, fmt.Errorf("issuer %q: attributes_cel: %w", config.GetId(), err)
		}
	}
	actor := config.GetActorCel()
	if actor == "" {
		actor = defaultActorExpression
	}
	if issuer.actor, err = compileMapping(claimsEnvironment, actor, types.StringKind); err != nil {
		return nil, fmt.Errorf("issuer %q: actor_cel: %w", config.GetId(), err)
	}
	if issuer.grants, err = compileMapping(grantsEnvironment, config.GetGrantsCel(), types.ListKind); err != nil {
		return nil, fmt.Errorf("issuer %q: grants_cel: %w", config.GetId(), err)
	}
	return issuer, nil
}

// newMappingEnvironments returns the environments of the mapping expressions: over `claims`, and for grants also
// over the `subject` and `attributes` computed from them.
func newMappingEnvironments() (*cel.Env, *cel.Env, error) {
	claimsEnvironment, err := newEnvironment(cel.Container(mappingContainer), cel.Variable("claims", claimsType))
	if err != nil {
		return nil, nil, fmt.Errorf("creating claims environment: %w", err)
	}
	grantsEnvironment, err := claimsEnvironment.Extend(
		cel.Variable("subject", cel.StringType),
		cel.Variable("attributes", claimsType),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating grants environment: %w", err)
	}
	return claimsEnvironment, grantsEnvironment, nil
}

// tokenAuthenticator proves principals by the tokens of the configured issuers.
type tokenAuthenticator struct {
	issuers []*issuer
}

// authenticate verifies a token and maps it to a session. A token that does not verify, or whose claims do not map,
// proves nothing: UNAUTHENTICATED. Only a fault of ours is INTERNAL.
func (a *tokenAuthenticator) authenticate(ctx context.Context, rawToken string) (*authenticationpb.Session, error) {
	unverified, err := readUnverifiedClaims(rawToken)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "parsing token: %v", err).Err()
	}
	issuer, err := a.match(unverified)
	if err != nil {
		return nil, err
	}
	keyOption, err := issuer.keys.verifyOption(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issuer %q: %v", issuer.config.GetId(), err).Err()
	}
	verifiedToken, err := jwt.Parse([]byte(rawToken),
		keyOption,
		jwt.WithIssuer(issuer.config.GetIssuer()),
		jwt.WithAudience(issuer.config.GetAudience()),
		jwt.WithRequiredClaim(jwt.ExpirationKey),
	)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verifying token: %v", err).Err()
	}
	session, err := issuer.session(verifiedToken)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "issuer %q: %v", issuer.config.GetId(), err).Err()
	}
	return session, nil
}

// match finds the one issuer naming both the token's `iss` and one of its audiences.
func (a *tokenAuthenticator) match(claims *unverifiedClaims) (*issuer, error) {
	audiences := claims.audiences()
	var matched *issuer
	for _, issuer := range a.issuers {
		if issuer.config.GetIssuer() != claims.Issuer || !slices.Contains(audiences, issuer.config.GetAudience()) {
			continue
		}
		if matched != nil {
			return nil, status.Errorf(codes.Unauthenticated, "token matches issuers %q and %q", matched.config.GetId(), issuer.config.GetId()).Err()
		}
		matched = issuer
	}
	if matched == nil {
		return nil, status.Errorf(codes.Unauthenticated, "untrusted issuer %q and audience %q", claims.Issuer, audiences).Err()
	}
	return matched, nil
}

// session maps verified claims through the issuer's expressions.
func (i *issuer) session(token jwt.Token) (*authenticationpb.Session, error) {
	// Claims reach expressions as v1 gave them to claims_rewrite_cel, so rewrites evaluate identically.
	claimsJSON, err := json.Marshal(token)
	if err != nil {
		return nil, fmt.Errorf("marshaling claims: %w", err)
	}
	claims := &structpb.Struct{}
	if err := claims.UnmarshalJSON(claimsJSON); err != nil {
		return nil, fmt.Errorf("unmarshaling claims: %w", err)
	}
	claimsMap := claims.AsMap()
	activation := map[string]any{"claims": claimsMap}
	subject, err := evaluateString(i.subject, activation)
	if err != nil {
		return nil, fmt.Errorf("subject: %w", err)
	}
	if subject == "" {
		return nil, fmt.Errorf("subject is empty")
	}
	attributes := claims
	if i.attributes != nil {
		if attributes, err = evaluateStruct(i.attributes, activation); err != nil {
			return nil, fmt.Errorf("attributes: %w", err)
		}
	}
	actor, err := evaluateString(i.actor, activation)
	if err != nil {
		return nil, fmt.Errorf("actor: %w", err)
	}
	grants, err := evaluateGrants(i.grants, map[string]any{
		"claims":     claimsMap,
		"subject":    subject,
		"attributes": attributes.AsMap(),
	})
	if err != nil {
		return nil, fmt.Errorf("grants: %w", err)
	}
	session := &authenticationpb.Session{
		Principal: &authenticationpb.Principal{Subject: subject, Attributes: attributes},
		Grants:    grants,
	}
	if actor != "" {
		session.Actor = &authenticationpb.Principal{Subject: actor}
	}
	return session, nil
}
