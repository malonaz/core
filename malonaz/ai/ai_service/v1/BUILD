grpc_library(
    name = "v1",
    srcs = [
        "ai_service.proto",
        "model.proto",
        "speech_to_text.proto",
        "text_to_speech.proto",
        "text_to_text.proto",
        "voice.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/ai_service/v1",
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
