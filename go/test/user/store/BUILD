subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/test/user/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//go/test/user/model",
        },
    },
    template = "//build_defs/codegen/go_proto_templates/templates:postgres",
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
