subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "user-service",
    test_only = True,
    deps = [
        "//go/test/user/user_service:manifest",
        "//malonaz/test/user/user_service/v1",
    ],
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
        "//go/pprof",
        "//go/prometheus",
        "//go/sentry",
        "//go/test/user/store",
        "//go/test/user/user_service",
        "//malonaz/test/user/user_service/v1",
    ],
)
