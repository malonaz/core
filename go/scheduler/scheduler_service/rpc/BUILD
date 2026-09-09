subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/scheduler/scheduler_service/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/scheduler/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "rpc",
    deps = ["//malonaz/scheduler/scheduler_service/v1"],
)

go_library(
    name = "rpc",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/scheduler/scheduler_service/rpc",
    visibility = ["//go/scheduler/scheduler_service/..."],
    deps = [
        "//go/aip",
        "//go/grpc/status",
        "//go/scheduler/model",
        "//go/uuid",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
