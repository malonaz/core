subinclude("//build_defs/codegen/onyx:build_defs")

onyx_deployment(
    name = "ai-service",
    manifest = "manifest.yaml",
    manifest_deps = [
        "//go/ai/ai_service:manifest",
        "//malonaz/ai/ai_service/v1",
    ],
    test_only = True,
)

go_binary(
    name = "ai-service",
    srcs = [":ai-service_main.go"],
    test_only = True,
    visibility = ["//go/test/ai/..."],
    deps = [
        "//go/ai/ai_service",
        "//go/ai/store",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//go/sentry",
        "//malonaz/ai/ai_service/v1",
    ],
)
