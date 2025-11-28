grpc_library(
    name = "v1",
    srcs = ["ai_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/ai_service/v1",
    },
    root_dir = "proto",
    visibility = ["PUBLIC"],
    deps = [
        "//genproto/ai/v1",
        "//genproto/audio/v1",
        "//proto/ai/v1",
        "//proto/audio/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
