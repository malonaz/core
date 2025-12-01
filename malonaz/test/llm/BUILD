subinclude("//build_defs/codegen/go_proto_templates:build_defs")

proto_library(
    name = "api",
    srcs = ["api.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/llm",
    },
    test_only = True,
    deps = [
        "//malonaz/codegen/llm/v1",
        "//third_party/proto:google__type__date",
        "//third_party/proto:google__type__postaladdress",
    ],
)

go_proto_template_library(
    name = "llm",
    srcs = ["api.proto"],
    template = "//build_defs/codegen/go_proto_templates/templates:llm",
    test_only = True,
    deps = [":api"],
)
