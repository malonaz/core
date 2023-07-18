subinclude("//build_defs:k8s")

k8s_config(
    name = "k8s",
    srcs = [
        "00-kube-state-metrics-rbac.yaml",
        "10-kube-state-metrics-cluster-role.yaml",
        "20-kube-state-metrics-deployment.yaml",
        "30-kube-state-metrics-service.yaml",
    ],
    visibility = [
        "//infrastructure/monitoring/...",
    ],
)
