subinclude("//build_defs/codegen/onyx:build_defs")

onyx_deployment(
    name = "ai-engine",
    manifest = "manifest.yaml",
    manifest_deps = [
        "//go/ai/ai_engine:manifest",
        "//malonaz/ai/ai_engine/v1",
    ],
    test_only = True,
)

go_binary(
    name = "ai-engine",
    srcs = [":ai-engine_main.go"],
    test_only = True,
    visibility = ["//go/test/ai/..."],
    deps = [
        "//go/ai/ai_engine",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/prometheus",
        "//malonaz/ai/ai_engine/v1",
        "//third_party/go:google.golang.org__grpc__reflection__grpc_reflection_v1",
    ],
)
