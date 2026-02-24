subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/library/v1:proto"],
    template = "//build_defs/codegen/go_proto_templates/templates:model",
    deps = ["//malonaz/library/v1"],
)

go_library(
    name = "model",
    srcs = [":codegen"],
    visibility = ["//go/library/..."],
    deps = [
        "//go/pbutil",
        "//malonaz/library/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
