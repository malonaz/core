filegroup(
    name = "proto",
    srcs = ["agent_service.proto"],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["agent_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/agent/agent_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/agent/v1",
        "//malonaz/ai/v1",
        "//malonaz/codegen/aip/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
