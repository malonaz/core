subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/test/user/user_service:manifest",
        "//malonaz/test/user/user_service/v1",
    ],
)

go_main_src(
    name = "main.go",
    manifest = ":manifest",
    test_only = True,
)

go_binary(
    name = "user-service",
    srcs = [":main.go"],
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
