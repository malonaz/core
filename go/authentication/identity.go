package authentication

import (
	"fmt"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

// IdentityOf returns the identity a session acts as, or false for an anonymous session.
func IdentityOf(session *authenticationpb.Session) (*authenticationpb.Identity, bool) {
	switch identity := session.GetIdentity().(type) {
	case *authenticationpb.Session_JwtIdentity:
		return &authenticationpb.Identity{Identity: &authenticationpb.Identity_JwtIdentity{JwtIdentity: identity.JwtIdentity}}, true
	case *authenticationpb.Session_ServiceAccountIdentity:
		return &authenticationpb.Identity{Identity: &authenticationpb.Identity_ServiceAccountIdentity{ServiceAccountIdentity: identity.ServiceAccountIdentity}}, true
	default:
		return nil, false
	}
}

// setIdentity makes a session act as an identity.
func setIdentity(session *authenticationpb.Session, identity *authenticationpb.Identity) error {
	switch identity := identity.GetIdentity().(type) {
	case *authenticationpb.Identity_JwtIdentity:
		session.Identity = &authenticationpb.Session_JwtIdentity{JwtIdentity: identity.JwtIdentity}
	case *authenticationpb.Identity_ServiceAccountIdentity:
		session.Identity = &authenticationpb.Session_ServiceAccountIdentity{ServiceAccountIdentity: identity.ServiceAccountIdentity}
	default:
		return fmt.Errorf("identity %T is neither a JWT nor a service account identity", identity)
	}
	return nil
}
