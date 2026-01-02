proto_library(
    name = "v1",
    srcs = [
        "jsonschema.proto",
        "message.proto",
        "metrics.proto",
        "model.proto",
        "tool.proto",
        "voice.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/v1",
        GENERATE_GO_AIP: True,
    },
    labels = ["copy_generated_code"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/audio/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:google__rpc__status",
        "//third_party/proto:validate",
    ],
)
