subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/workspace/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/workspace/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    deps = ["//malonaz/workspace/v1"],
)

go_library(
    name = "store",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/workspace/store",
    visibility = ["PUBLIC"],
    deps = [
        "//go/postgres",
        "//go/workspace/model",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__jackc__pgerrcode",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
    ],
)
