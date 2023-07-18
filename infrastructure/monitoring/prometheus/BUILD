subinclude("//build_defs:k8s")

k8s_config(
    name = "k8s",
    srcs = [
        "00-prometheus-config-map.yaml",
        "10-prometheus-deployment.yaml",
        "20-prometheus-svc.yaml",
        "backend-config.yaml",
    ],
    visibility = [
        "//infrastructure/monitoring/...",
    ],
)
