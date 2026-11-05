filegroup(
    name = "proto",
    srcs = [
        "agent.proto",
        "agent_service.proto",
        "builtin_tool.proto",
        "definition.proto",
    ],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = [
        "agent.proto",
        "agent_service.proto",
        "builtin_tool.proto",
        "definition.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/agent/agent_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/ai/agent/v1",
        "//malonaz/ai/v1",
        "//malonaz/aip/v1",
        "//malonaz/codegen/aip/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
