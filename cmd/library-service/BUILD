subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/library/library_service:manifest",
        "//malonaz/library/library_service/v1",
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
    visibility = ["//go/library/library_service/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/library/library_service",
        "//go/library/store",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//malonaz/library/library_service/v1",
    ],
)
