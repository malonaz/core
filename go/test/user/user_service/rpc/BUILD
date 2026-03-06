subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/test/user/user_service/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//go/test/user/model",
        },
    },
    template = "//build_defs/codegen/go_proto_templates/templates:rpc",
    test_only = True,
    deps = ["//malonaz/test/user/user_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    test_only = True,
    visibility = ["//go/test/user/user_service/..."],
    deps = [
        "//go/aip",
        "//go/grpc/status",
        "//go/nats",
        "//go/test/user/model",
        "//go/uuid",
        "//malonaz/test/user/user_service/v1",
        "//malonaz/test/user/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
