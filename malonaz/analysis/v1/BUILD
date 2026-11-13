filegroup(
    name = "proto",
    srcs = [
        "analysis.proto",
        "annotations.proto",
        "labels.proto",
        "result.proto",
    ],
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "analysis.proto",
        "annotations.proto",
        "labels.proto",
        "result.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/analysis/v1",
        GENERATE_GO_AIP: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/ai/v1",
        "//malonaz/codegen/aip/v1",
        "//malonaz/json/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
