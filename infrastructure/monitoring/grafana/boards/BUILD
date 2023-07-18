subinclude("//build_defs:grafana")

grafana_json(
    name = "pod_overview",
    src = "pod_overview.json",
    folder = "Kubernetes",
)

grafana_json(
    name = "cluster_overview",
    src = "cluster_overview.json",
    folder = "Kubernetes",
)

grafana_python(
    name = "deployment_overview",
    src = "deployment_overview.py",
    folder = "Kubernetes",
    deps = [
        "//common/python/grafana",
        "//third_party/python:grafanalib",
    ],
)
