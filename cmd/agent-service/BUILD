subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/agent/agent_service:manifest",
        "//malonaz/agent/agent_service/v1",
    ],
)

go_main_src(
    name = "main.go",
    manifest = ":manifest",
    test_only = True,
)

go_binary(
    name = "agent-service",
    srcs = [":main.go"],
    test_only = True,
    visibility = ["//go/test/..."],
    deps = [
        "//go/agent/agent_service",
        "//go/agent/store",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//malonaz/agent/agent_service/v1",
        "//malonaz/ai/ai_service/v1",
    ],
)
