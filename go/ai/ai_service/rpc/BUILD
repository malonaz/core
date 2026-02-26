subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/ai/ai_service/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//go/ai/model",
        },
    },
    template = "//build_defs/codegen/go_proto_templates/templates:rpc",
    deps = ["//malonaz/ai/ai_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    visibility = ["//go/ai/ai_service/..."],
    deps = [
        "//go/ai/model",
        "//go/aip",
        "//go/grpc",
        "//go/uuid",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
