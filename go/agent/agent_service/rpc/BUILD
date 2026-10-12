subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/agent/agent_service/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/agent/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "rpc",
    deps = ["//malonaz/agent/agent_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/agent/agent_service/rpc",
    visibility = ["//go/agent/agent_service/..."],
    deps = [
        "//go/agent/model",
        "//go/aip",
        "//go/grpc/status",
        "//go/uuid",
        "//malonaz/agent/agent_service/v1",
        "//malonaz/agent/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
