subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/ai/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//gengo/ai/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    template = "//build_defs/codegen/go_proto_templates/templates:postgres",
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
