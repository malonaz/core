package manifest

import (
	"fmt"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
)

// Fixed by the health and prometheus opts of every binary.
const (
	HealthPort     = 4040
	PrometheusPort = 13434
)

// Default ports of each listener, matching their opts' defaults.
const (
	defaultGRPCPort         = 9090
	defaultGatewayPort      = 8080
	defaultHTTPPort         = 8080
	defaultGRPCWebProxyPort = 8443
)

// Listener is what a server of the binary binds: a TCP port, or a socket for an internal grpc server.
type Listener struct {
	// The flag namespace (`{server}-grpc`, ...), which also names its Service port and Ingress.
	Namespace string
	// Set exactly when SocketPath is not.
	Port       int32
	SocketPath string
	// Set when the manifest overrides the opts' default port.
	CustomPort bool
	// A grpc server's own listener; the gateway, http and grpc_web_proxy serve HTTP.
	GRPC     bool
	Internal bool
	Public   bool
}

// Listeners returns what the servers bind, in server order.
func Listeners(servers []*onyxpb.Server) ([]*Listener, error) {
	var listeners []*Listener
	listen := func(server *onyxpb.Server, namespace string, port, defaultPort int32, grpc bool) {
		listener := &Listener{Namespace: namespace, Port: port, CustomPort: port != 0, GRPC: grpc, Internal: server.GetInternal(), Public: server.GetPublic()}
		if port == 0 {
			listener.Port = defaultPort
		}
		listeners = append(listeners, listener)
	}

	for _, server := range servers {
		name := server.GetName()
		switch kind := server.GetKind().(type) {
		case *onyxpb.Server_Grpc_:
			namespace := name + "-grpc"
			if server.GetInternal() {
				listeners = append(listeners, &Listener{Namespace: namespace, SocketPath: "/tmp/" + name + ".socket", GRPC: true, Internal: true})
			} else {
				listen(server, namespace, server.GetPort(), defaultGRPCPort, true)
			}
			for _, service := range kind.Grpc.GetServices() {
				if service.GetGateway() {
					listen(server, name+"-grpc-gateway", kind.Grpc.GetGatewayPort(), defaultGatewayPort, false)
					break
				}
			}
		case *onyxpb.Server_Http_:
			listen(server, name+"-http", server.GetPort(), defaultHTTPPort, false)
		case *onyxpb.Server_GrpcWebProxy_:
			listen(server, name+"-grpcwebproxy", server.GetPort(), defaultGRPCWebProxyPort, false)
		case *onyxpb.Server_Processor_:
		default:
			return nil, fmt.Errorf("server %s: unknown kind %T", name, kind)
		}
	}

	return listeners, nil
}
