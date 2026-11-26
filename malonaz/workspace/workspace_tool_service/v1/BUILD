filegroup(
    name = "proto",
    srcs = ["workspace_tool_service.proto"],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["workspace_tool_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/workspace/workspace_tool_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/workspace/workspace_service/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
