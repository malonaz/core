subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/agent/v1:proto"],
    labels = ["copy_generated_code:go:gengo"],
    plugin = "model",
    deps = ["//malonaz/agent/v1"],
)

go_library(
    name = "model",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/agent/model",
    visibility = ["PUBLIC"],
    deps = [
        "//go/pbutil",
        "//malonaz/agent/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/proto:google__rpc__status",  # wollemi:keep
    ],
)
