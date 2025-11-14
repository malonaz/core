subinclude("//build_defs/codegen/go_proto_templates:build_defs")

proto_library(
    name = "v1",
    srcs = ["job.proto"],
    visibility = ["PUBLIC"],
    deps = [
        "//genproto/codegen/model/v1",
        "//third_party/proto:core__codegen__model__v1",
        "//third_party/proto:google__api__annotations",
    ],
)

go_proto_template_library(
    name = "model",
    srcs = ["job.proto"],
    template = "//build_defs/codegen/go_proto_templates/templates:model",
    visibility = ["PUBLIC"],
    deps = [":v1"],
)

go_proto_template_library(
    name = "db",
    srcs = ["job.proto"],
    template = "//build_defs/codegen/go_proto_templates/templates:db",
    visibility = ["PUBLIC"],
    deps = [":v1"],
)
