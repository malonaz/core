package authentication

import (
	"fmt"

	authenticationpb "github.com/malonaz/core/genproto/authentication"
)

// Helper functions to convert session data to sets for O(1) lookup
func (i *Interceptor) getUserPermissionSet(session *authenticationpb.Session) (map[string]struct{}, error) {
	permissions := make(map[string]struct{})
	for _, roleID := range session.RoleIds {
		role, ok := i.roleIDToRole[roleID]
		if !ok {
			return nil, fmt.Errorf("invalid user role id %s", roleID)
		}
		for _, permission := range role.Permissions {
			permissions[permission] = struct{}{}
		}
	}
	return permissions, nil
}

func (i *Interceptor) getUserRoleIDSet(session *authenticationpb.Session) map[string]struct{} {
	roles := make(map[string]struct{})
	for _, roleID := range session.RoleIds {
		roles[roleID] = struct{}{}
	}
	return roles
}
