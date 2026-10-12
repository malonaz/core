filegroup(
    name = "proto",
    srcs = [
        "annotations.proto",
        "eval.proto",
        "evaluation.proto",
        "labels.proto",
    ],
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "annotations.proto",
        "eval.proto",
        "evaluation.proto",
        "labels.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/eval/v1",
        GENERATE_GO_AIP: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/aip/v1",
        "//malonaz/codegen/aip/v1",
        "//third_party/proto:validate",
    ],
)
