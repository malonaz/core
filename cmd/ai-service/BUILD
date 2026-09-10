subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/ai/ai_service:manifest",
        "//malonaz/ai/ai_service/v1",
    ],
)

onyx_main_src(
    name = "main.go",
    manifest = ":manifest",
    test_only = True,
)

go_binary(
    name = "ai-service",
    srcs = [":main.go"],
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
        "//malonaz/ai/ai_service/v1",
    ],
)
