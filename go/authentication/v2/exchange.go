package authentication

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

// Subjects is what the host knows of the subjects its issuers' tokens are for.
type Subjects interface {
	// Claims returns the claims a token of the issuer carries for the subject now: what the issuer's mapping reads.
	// An error refuses the exchange with its code, NotFound for a subject the host does not know.
	Claims(ctx context.Context, issuer, subject string) (map[string]any, error)
}

// TokenService serves malonaz.authentication.v2.TokenService. It must be served behind the interceptor, whose rules
// decide who exchanges.
type TokenService struct {
	authenticationpb.UnimplementedTokenServiceServer
	subjects           Subjects
	issuerIDToExchange map[string]*exchange
}

// exchange is an issuer whose tokens are exchanged for: its mapping, its signer and their lifetime.
type exchange struct {
	issuer   *issuer
	signer   *Signer
	lifetime time.Duration
}

// NewTokenService returns the service exchanging for every configured issuer with an `exchange_lifetime`, each signing
// with its key, given here as NewSigner takes it: the configuration holds only what verification needs.
func NewTokenService(ctx context.Context, configuration *authenticationpb.Configuration, subjects Subjects, issuerIDToKey map[string]string) (*TokenService, error) {
	claimsEnvironment, grantsEnvironment, err := newMappingEnvironments()
	if err != nil {
		return nil, err
	}
	service := &TokenService{subjects: subjects, issuerIDToExchange: map[string]*exchange{}}
	for _, config := range configuration.GetIssuers() {
		if config.GetExchangeLifetime() == nil {
			continue
		}
		key, ok := issuerIDToKey[config.GetId()]
		if !ok {
			return nil, fmt.Errorf("issuer %q is exchanged for, but no key is given to sign its tokens", config.GetId())
		}
		issuer, err := newIssuer(ctx, config, claimsEnvironment, grantsEnvironment)
		if err != nil {
			return nil, err
		}
		signer, err := NewSigner(config, key)
		if err != nil {
			return nil, err
		}
		service.issuerIDToExchange[config.GetId()] = &exchange{issuer: issuer, signer: signer, lifetime: config.GetExchangeLifetime().AsDuration()}
	}
	for issuerID := range issuerIDToKey {
		if _, ok := service.issuerIDToExchange[issuerID]; !ok {
			return nil, fmt.Errorf("a key is given for issuer %q, which has no exchange_lifetime", issuerID)
		}
	}
	return service, nil
}

// ExchangeToken issues a token of the issuer for the subject, made by the actor, carrying the host's claims for the
// subject now.
func (s *TokenService) ExchangeToken(ctx context.Context, request *authenticationpb.ExchangeTokenRequest) (*authenticationpb.ExchangeTokenResponse, error) {
	session, err := GetSession(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "the token service is served without the authentication interceptor").Err()
	}
	// Only a service calling as itself: never a session carried for another principal, an outside credential, or an
	// actor, which every exchanged token has (no chains).
	principal, actor, caller := session.GetPrincipal().GetSubject(), session.GetActor().GetSubject(), session.GetCaller().GetSubject()
	if caller == "" || principal != caller || actor != "" {
		return nil, status.Errorf(codes.PermissionDenied, "only a service calling as itself exchanges, not principal %q acted for by %q through caller %q", principal, actor, caller).Err()
	}
	exchange, ok := s.issuerIDToExchange[request.GetIssuer()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "issuer %q is not exchanged for", request.GetIssuer()).Err()
	}
	claims, err := s.subjects.Claims(ctx, request.GetIssuer(), request.GetSubject())
	if err != nil {
		return nil, status.FromError(err, "claims of %s", request.GetSubject()).Err()
	}
	for _, name := range signerClaims {
		if _, ok := claims[name]; ok {
			return nil, status.Errorf(codes.Internal, "the claims of %s name %q, which the exchange writes", request.GetSubject(), name).Err()
		}
	}
	lifetime := exchange.lifetime
	if request.GetLifetime() != nil {
		lifetime = min(lifetime, request.GetLifetime().AsDuration())
	}
	token, expireTime, err := exchange.signer.build(Token{Subject: request.GetSubject(), Actor: request.GetActor(), Lifetime: lifetime, Claims: claims})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	// The issuer's mapping may read claims other than `sub` and `act`: the token must prove exactly whom it was asked for.
	exchanged, err := exchange.issuer.session(token)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issuer %q: %v", request.GetIssuer(), err).Err()
	}
	if subject, actor := exchanged.GetPrincipal().GetSubject(), exchanged.GetActor().GetSubject(); subject != request.GetSubject() || actor != request.GetActor() {
		return nil, status.Errorf(codes.Internal, "issuer %q maps the token to %q acted for by %q", request.GetIssuer(), subject, actor).Err()
	}
	accessToken, err := signToken(token, exchange.signer.key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	return &authenticationpb.ExchangeTokenResponse{AccessToken: accessToken, ExpireTime: timestamppb.New(expireTime)}, nil
}
