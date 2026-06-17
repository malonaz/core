subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/test/user/user_service/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//go/test/user/model",
        },
    },
    plugin = "rpc",
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
        "//go/pbutil",
        "//go/test/user/model",
        "//go/uuid",
        "//malonaz/codegen/nats/v1",
        "//malonaz/nats/v1",
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
