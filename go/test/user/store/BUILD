subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/test/user/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//go/test/user/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    test_only = True,
    deps = ["//malonaz/test/user/v1"],
)

go_library(
    name = "store",
    srcs = [":codegen"],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/postgres",
        "//go/test/user/model",
        "//third_party/go:github.com__jackc__pgx__v5",
    ],
)
