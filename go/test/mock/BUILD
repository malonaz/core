subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/test/mock/v1:proto"],
    labels = ["copy_generated_code:go:gengo"],
    plugin = "mock",
    test_only = True,
    deps = ["//malonaz/test/mock/v1"],
)

go_library(
    name = "mock",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/test/mock",
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/grpc/mock_server",
        "//malonaz/test/mock/v1",
    ],
)
