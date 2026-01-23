go_binary(
    name = "grafana-upload-alert-rules",
    srcs = ["main.go"],
    visibility = ["PUBLIC"],
    deps = [
        "//go/flags",
        "//go/grafana",
        "//go/jsonnet",
        "//go/logging",
        "//third_party/go:github.com__grafana__grafana-openapi-client-go__client__provisioning",
        "//third_party/go:github.com__grafana__grafana-openapi-client-go__models",
    ],
)
