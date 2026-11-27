subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "workspace-agent",
    deps = [
        "//go/workspace/node_service:manifest",
        "//malonaz/workspace/node_service/v1",
    ],
)

go_binary(
    name = "workspace-agent",
    srcs = [":workspace-agent_main.go"],
    visibility = ["PUBLIC"],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/pprof",
        "//go/prometheus",
        "//go/sentry",
        "//go/workspace/node_service",
        "//malonaz/workspace/node_service/v1",
    ],
)
