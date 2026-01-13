grpc_library(
    name = "v1",
    srcs = ["ai_engine.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/ai_engine/v1",
    },
    labels = ["copy_generated_code"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/ai/v1",
        "//malonaz/audio/v1",
        "//malonaz/codegen/aip/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
