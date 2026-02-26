proto_library(
    name = "v1",
    srcs = [
        "model.proto",
        "onyx.proto",
        "service.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/onyx/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:validate",
    ],
)
