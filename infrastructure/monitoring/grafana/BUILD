subinclude("//build_defs:k8s")

k8s_config(
    name = "k8s",
    srcs = [
        "00-grafana-config-map.yaml",
        "20-grafana-svc.yaml",
    ],
    visibility = [
        "//infrastructure/monitoring/...",
    ],
)

k8s_config(
    name = "prod_k8s",
    srcs = [
        "10-grafana-deployment.yaml",
        "backend-config.yaml",
        "database-configmap.yaml",
    ],
    visibility = ["//infrastructure/monitoring/..."],
)

k8s_config(
    name = "dev_k8s",
    srcs = ["deployment-minikube.yaml"],
    visibility = ["//infrastructure/monitoring/..."],
)
