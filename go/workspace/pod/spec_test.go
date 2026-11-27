package pod

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc"
)

const (
	testNamespace     = "workspaces"
	testWorkspaceName = "organizations/o/users/u/workspaces/w"
	repoFilesystem    = "organizations/o/users/u/filesystems/repo"
	sharedFilesystem  = "organizations/o/users/u/filesystems/shared"
)

func testConfig() *Config {
	return &Config{
		Namespace:  testNamespace,
		Image:      "workspace-server:1",
		DisableTLS: true,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		},
		Env: []corev1.EnvVar{{Name: "WORKSPACE_SERVICE_AUTHENTICATION", Value: "none"}},
	}
}

func testWorkspace() *pb.Workspace {
	return &pb.Workspace{
		Name: testWorkspaceName,
		Mounts: []*pb.Mount{
			{Filesystem: repoFilesystem, Path: "/workspace", Mode: pb.Mount_MODE_READ_WRITE},
			{Filesystem: sharedFilesystem, Path: "/shared/me", Mode: pb.Mount_MODE_READ_ONLY},
		},
	}
}

var testFilesystemNameToKind = map[string]pb.Filesystem_Kind{
	repoFilesystem:   pb.Filesystem_KIND_LOCAL,
	sharedFilesystem: pb.Filesystem_KIND_SHARED,
}

func TestName(t *testing.T) {
	name := Name(testWorkspaceName)
	require.Len(t, name, len("ws-")+20)
	require.Equal(t, name, Name(testWorkspaceName))
	require.NotEqual(t, name, Name("organizations/o/users/other/workspaces/w"))
}

func TestSpec(t *testing.T) {
	t.Run("mounts", func(t *testing.T) {
		pod, err := Spec(testConfig(), testWorkspace(), testFilesystemNameToKind)
		require.NoError(t, err)

		readWrite, readOnly, disabled := false, true, false
		runtimeClassName := "gvisor"
		tmpSizeLimit := resource.MustParse("4Gi")
		expected := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        Name(testWorkspaceName),
				Namespace:   testNamespace,
				Labels:      map[string]string{LabelName: "workspace"},
				Annotations: map[string]string{AnnotationWorkspace: testWorkspaceName},
			},
			Spec: corev1.PodSpec{
				RuntimeClassName:             &runtimeClassName,
				AutomountServiceAccountToken: &disabled,
				EnableServiceLinks:           &disabled,
				Containers: []corev1.Container{{
					Name:  "workspace-server",
					Image: "workspace-server:1",
					Env: []corev1.EnvVar{
						{Name: "WORKSPACE_SERVICE_DIRECTORY", Value: "/workspace"},
						{Name: "WORKSPACE_SERVICE_GRPC_PORT", Value: "9090"},
						{Name: "WORKSPACE_SERVICE_GRPC_DISABLE_TLS", Value: "true"},
						{Name: "WORKSPACE_SERVICE_AUTHENTICATION", Value: "none"},
					},
					Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: 9090}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "tmp", MountPath: "/tmp"},
						{Name: "mount-0", MountPath: "/workspace"},
						{Name: "mount-1", MountPath: "/shared/me", ReadOnly: true},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("grpc")}},
						PeriodSeconds: 1,
					},
				}},
				Volumes: []corev1.Volume{
					{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmpSizeLimit}}},
					{Name: "mount-0", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver:           "workspaces.onikisu",
						ReadOnly:         &readWrite,
						VolumeAttributes: map[string]string{"filesystem": repoFilesystem, "kind": "local", "readOnly": "false"},
					}}},
					{Name: "mount-1", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver:           "workspaces.onikisu",
						ReadOnly:         &readOnly,
						VolumeAttributes: map[string]string{"filesystem": sharedFilesystem, "kind": "shared", "readOnly": "true"},
					}}},
				},
			},
		}
		require.Equal(t, expected, pod)
	})

	t.Run("no mounts runs in /tmp", func(t *testing.T) {
		workspace := &pb.Workspace{Name: testWorkspaceName}
		pod, err := Spec(testConfig(), workspace, nil)
		require.NoError(t, err)
		require.Equal(t, corev1.EnvVar{Name: "WORKSPACE_SERVICE_DIRECTORY", Value: "/tmp"}, pod.Spec.Containers[0].Env[0])
		require.Len(t, pod.Spec.Volumes, 1)
	})

	t.Run("unknown filesystem kind", func(t *testing.T) {
		_, err := Spec(testConfig(), testWorkspace(), map[string]pb.Filesystem_Kind{repoFilesystem: pb.Filesystem_KIND_LOCAL})
		require.ErrorContains(t, err, "no kind for filesystem "+sharedFilesystem)
	})
}

func TestClientOpts(t *testing.T) {
	config := testConfig()
	pod, err := Spec(config, testWorkspace(), testFilesystemNameToKind)
	require.NoError(t, err)

	_, err = config.ClientOpts(pod)
	require.ErrorContains(t, err, "has no IP")

	clientOpts, err := config.ClientOpts(running(pod, time.Now()))
	require.NoError(t, err)
	require.Equal(t, &grpc.ClientOpts{Host: "10.0.0.7", Port: 9090, DisableTLS: true}, clientOpts)
}
