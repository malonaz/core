subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    test_only = True,
    visibility = ["//cmd/..."],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
    test_only = True,
)

go_library(
    name = "library_service",
    srcs = [
        "runtime.go",
        ":service.tmpl.go",
    ],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/grpc/middleware",
        "//go/test/library/library_service/rpc",
        "//go/test/library/store",
        "//go/nats",
        "//malonaz/test/library/v1",
        "//third_party/go:google.golang.org__grpc__metadata",
    ],
)
