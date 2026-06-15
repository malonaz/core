subinclude("//build_defs/codegen/go_proto_templates:build_defs")

go_proto_template_library(
    name = "codegen",
    srcs = ["//malonaz/test/library/library_service/v1:proto"],
    additional_context = {
        GO_PROTO_TEMPLATES_GO_IMPORT_PATHS: {
            "model": "//go/test/library/model",
        },
    },
    template = "//build_defs/codegen/go_proto_templates/templates:rpc",
    test_only = True,
    deps = ["//malonaz/test/library/library_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    test_only = True,
    visibility = ["//go/test/library/library_service/..."],
    deps = [
        "//go/aip",
        "//go/grpc/status",
        "//go/nats",
        "//go/pbutil",
        "//go/test/library/model",
        "//go/uuid",
        "//malonaz/codegen/nats/v1",
        "//malonaz/nats/v1",
        "//malonaz/test/library/library_service/v1",
        "//malonaz/test/library/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:github.com__google__cel-go__cel",
        "//third_party/go:github.com__google__cel-go__ext",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
