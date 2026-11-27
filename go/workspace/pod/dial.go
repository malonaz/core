package pod

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/malonaz/core/go/certs"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/prometheus"
)

// Dialer connects to the workspace-server of running pods.
type Dialer struct {
	port           int
	disableTLS     bool
	certsOpts      *certs.Opts
	prometheusOpts *prometheus.Opts
}

// NewDialer returns a dialer for workspace-servers serving on port.
func NewDialer(port int, disableTLS bool, certsOpts *certs.Opts, prometheusOpts *prometheus.Opts) *Dialer {
	return &Dialer{port: port, disableTLS: disableTLS, certsOpts: certsOpts, prometheusOpts: prometheusOpts}
}

// ClientOpts addresses a pod's workspace-server by its IP: pods have no DNS name.
func (d *Dialer) ClientOpts(pod *corev1.Pod) (*grpc.ClientOpts, error) {
	if pod.Status.PodIP == "" {
		return nil, fmt.Errorf("pod %s has no IP", pod.Name)
	}
	clientOpts := &grpc.ClientOpts{
		Host:       pod.Status.PodIP,
		Port:       d.port,
		DisableTLS: d.disableTLS,
	}
	return clientOpts, nil
}

// Dial connects to a pod's workspace-server: the caller builds the WorkspaceService client on the
// connection and closes it.
func (d *Dialer) Dial(ctx context.Context, pod *corev1.Pod) (*grpc.Connection, error) {
	clientOpts, err := d.ClientOpts(pod)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewConnection(clientOpts, d.certsOpts, d.prometheusOpts)
	if err != nil {
		return nil, fmt.Errorf("building connection to pod %s: %w", pod.Name, err)
	}
	if err := connection.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connecting to pod %s: %w", pod.Name, err)
	}
	return connection, nil
}
