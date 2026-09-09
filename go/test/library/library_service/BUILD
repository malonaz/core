subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    test_only = True,
    visibility = ["//cmd/..."],
    deps = ["//malonaz/scheduler/scheduler_service/v1"],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
    test_only = True,
)

go_library(
    name = "library_service",
    srcs = [
        "import_books.go",
        "runtime.go",
        ":service.tmpl.go",
    ],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/grpc/middleware",
        "//go/grpc/status",
        "//go/nats",
        "//go/scheduler/longrunning",
        "//go/test/library/library_service/rpc",
        "//go/test/library/store",
        "//go/uuid",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/test/library/library_service/v1",
        "//malonaz/test/library/v1",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
    ],
)
