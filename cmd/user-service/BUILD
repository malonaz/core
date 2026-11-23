subinclude("//build_defs/codegen/onyx:build_defs")

onyx_deployment(
    name = "user-service",
    manifest = "manifest.yaml",
    manifest_deps = [
        "//go/test/user/user_service:manifest",
        "//malonaz/test/user/user_service/v1",
    ],
    test_only = True,
)

go_binary(
    name = "user-service",
    srcs = [":user-service_main.go"],
    test_only = True,
    visibility = ["//go/test/user/user_service/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/nats",
        "//go/postgres",
        "//go/prometheus",
        "//go/test/user/store",
        "//go/test/user/user_service",
        "//malonaz/test/user/user_service/v1",
    ],
)
