subinclude("//build_defs:k8s")

subinclude("//build_defs:crown_package")

crown_package(
    dev_k8s = [
        "//infrastructure/monitoring/grafana:dev_k8s",
    ],
    k8s = [
        "//infrastructure/monitoring/grafana:k8s",
        "//infrastructure/monitoring/kubernetes/kube-state-metrics:k8s",
        "//infrastructure/monitoring/kubernetes/node-exporter:k8s",
        "//infrastructure/monitoring/prometheus:k8s",
    ],
    prod_k8s = [
        "//infrastructure/monitoring/grafana:prod_k8s",
    ],
)
