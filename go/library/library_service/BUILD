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
    srcs = [":service.tmpl.go"],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/library/library_service/rpc",
        "//go/library/store",
    ],
)
