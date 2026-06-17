subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/ai/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/ai/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    deps = ["//malonaz/ai/v1"],
)

go_library(
    name = "store",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/ai/store",
    visibility = ["//..."],
    deps = [
        "//go/ai/model",
        "//go/postgres",
        "//third_party/go:github.com__jackc__pgx__v5",
    ],
)
