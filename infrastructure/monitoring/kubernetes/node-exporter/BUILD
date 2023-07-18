subinclude("//build_defs:k8s")

k8s_config(
    name = "k8s",
    srcs = [
        "00-node-exporter-deployment.yaml",
        "10-node-exporter-svc.yaml",
    ],
    visibility = [
        "//infrastructure/monitoring/...",
    ],
)
