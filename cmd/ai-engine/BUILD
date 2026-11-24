subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "ai-engine",
    test_only = True,
    deps = [
        "//go/ai/ai_engine:manifest",
        "//malonaz/ai/ai_engine/v1",
    ],
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
        "//go/sentry",
        "//malonaz/ai/ai_engine/v1",
        "//third_party/go:google.golang.org__grpc__reflection__grpc_reflection_v1",
    ],
)
