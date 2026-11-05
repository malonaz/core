filegroup(
    name = "proto",
    srcs = ["workspace_service.proto"],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["workspace_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/workspace/workspace_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
