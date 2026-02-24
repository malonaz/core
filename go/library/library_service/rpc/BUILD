subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/library/library_service/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//go/library/model",
        },
    },
    template = "//build_defs/codegen/go_proto_templates/templates:rpc",
    test_only = True,
    deps = ["//malonaz/library/library_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    test_only = True,
    visibility = ["//go/library/library_service/..."],
    deps = [
        "//go/aip",
        "//go/grpc",
        "//go/library/model",
        "//go/uuid",
        "//malonaz/library/library_service/v1",
        "//malonaz/library/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
