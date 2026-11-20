package authentication

import (
	"context"
	"fmt"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/middleware"
)

// injectLogFields adds the session to the call's log: its principal, actor, caller, the principal's attributes
// (`attribute.{key}`, formatted as v1 formatted `claim.{key}`) and the client.
func injectLogFields(ctx context.Context, session *authenticationpb.Session) {
	var fields []any
	if subject := session.GetPrincipal().GetSubject(); subject != "" {
		fields = append(fields, "principal", subject)
	}
	if subject := session.GetActor().GetSubject(); subject != "" {
		fields = append(fields, "actor", subject)
	}
	if subject := session.GetCaller().GetSubject(); subject != "" {
		fields = append(fields, "caller", subject)
	}
	for key, value := range session.GetPrincipal().GetAttributes().AsMap() {
		fields = append(fields, "attribute."+key, fmt.Sprintf("%v", value))
	}
	client := session.GetClient()
	if ipAddress := client.GetIpAddress(); ipAddress != "" {
		fields = append(fields, "ip_address", ipAddress)
	}
	if platform := client.GetPlatform(); platform != "" {
		fields = append(fields, "client_platform", platform)
	}
	if version := client.GetVersion(); version != nil {
		fields = append(fields, "client_version", fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch))
	}
	if userAgent := client.GetUserAgent(); userAgent != "" {
		fields = append(fields, "user_agent", userAgent)
	}
	if len(fields) > 0 {
		middleware.InjectLogFields(ctx, fields...)
	}
}
