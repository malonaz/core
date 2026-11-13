filegroup(
    name = "proto",
    srcs = ["analysis_service.proto"],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["analysis_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/analysis/analysis_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/analysis/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
