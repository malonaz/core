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
    name = "user_service",
    srcs = [":service.tmpl.go"],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/nats",
        "//go/test/user/store",
        "//go/test/user/user_service/rpc",
    ],
)
