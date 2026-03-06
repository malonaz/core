subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/test/library/library_service:manifest",
        "//malonaz/test/library/library_service/v1",
    ],
)

go_main_src(
    name = "main.go",
    manifest = ":manifest",
    test_only = True,
)

go_binary(
    name = "library-service",
    srcs = [":main.go"],
    test_only = True,
    visibility = ["//go/test/library/library_service/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/nats",
        "//go/postgres",
        "//go/prometheus",
        "//go/test/library/library_service",
        "//go/test/library/store",
        "//malonaz/test/library/library_service/v1",
    ],
)
