subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/test/library/v1:proto"],
    labels = ["copy_generated_code:go:gengo"],
    plugin = "model",
    test_only = True,
    deps = ["//malonaz/test/library/v1"],
)

go_library(
    name = "model",
    srcs = [":codegen"],
    test_only = True,
    visibility = ["//go/test/library/..."],
    deps = [
        "//go/pbutil",
        "//malonaz/test/library/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
