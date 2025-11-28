proto_library(
    name = "v1",
    srcs = [
        "jsonschema.proto",
        "message.proto",
        "metrics.proto",
        "model.proto",
        "tool.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/v1",
    },
    root_dir = "proto",
    visibility = ["PUBLIC"],
    deps = [
        "//genproto/audio/v1",
        "//proto/audio/v1",
        "//third_party/proto:validate",
    ],
)
