package authentication

import (
	"context"
	"fmt"

	"github.com/malonaz/core/go/grpc/middleware"
)

// injectLogFields adds the authenticated call to its log: the principal, the issuer whose token proved it, actor,
// caller, the principal's attributes (`attribute.{key}`) and the client.
func injectLogFields(ctx context.Context, call *authenticatedCall) {
	session := call.session
	var fields []any
	if subject := session.GetPrincipal().GetSubject(); subject != "" {
		fields = append(fields, "principal", subject)
	}
	if call.issuer != "" {
		fields = append(fields, "issuer", call.issuer)
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
