package filesystem_service

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	"github.com/malonaz/core/go/grpc"
)

// agentLabelValue selects the workspace agent's pods by app.kubernetes.io/name.
const agentLabelValue = "workspace-agent"

// nodes reaches the workspace agent running on a node.
type nodes interface {
	// Client dials the agent of a node, or of any node when node is empty. close releases it.
	Client(ctx context.Context, node string) (client nodeservicepb.NodeServiceClient, close func(), err error)
}

// agentNodes finds a node's agent among the agent DaemonSet's pods and dials its IP.
type agentNodes struct {
	pods typedcorev1.PodInterface
	opts *Opts
}

func newAgentNodes(clientset kubernetes.Interface, opts *Opts) *agentNodes {
	return &agentNodes{pods: clientset.CoreV1().Pods(opts.AgentNamespace), opts: opts}
}

// Client implements nodes.
func (n *agentNodes) Client(ctx context.Context, node string) (nodeservicepb.NodeServiceClient, func(), error) {
	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{"app.kubernetes.io/name": agentLabelValue}).String(),
		FieldSelector: fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)).String(),
	}
	if node != "" {
		listOptions.FieldSelector = fields.AndSelectors(
			fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)),
			fields.OneTermEqualSelector("spec.nodeName", node),
		).String()
	}
	podList, err := n.pods.List(ctx, listOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("listing agent pods: %w", err)
	}
	var host string
	for _, pod := range podList.Items {
		if pod.Status.PodIP != "" {
			host = pod.Status.PodIP
			break
		}
	}
	if host == "" {
		return nil, nil, fmt.Errorf("no running agent on node %q", node)
	}

	clientOpts := &grpc.ClientOpts{Host: host, Port: n.opts.AgentPort, DisableTLS: n.opts.AgentDisableTLS}
	connection, err := grpc.NewConnection(clientOpts, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("creating agent connection: %w", err)
	}
	if err := connection.Connect(ctx); err != nil {
		return nil, nil, fmt.Errorf("connecting to agent %s: %w", host, err)
	}
	close := func() { connection.Close() }
	return nodeservicepb.NewNodeServiceClient(connection.Get()), close, nil
}
