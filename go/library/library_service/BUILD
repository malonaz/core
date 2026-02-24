subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["//cmd/..."],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
)

go_library(
    name = "library_service",
    srcs = [":service.tmpl.go"],
    visibility = ["//..."],
    deps = [
        "//go/library/library_service/rpc",
        "//go/library/store",
    ],
)
