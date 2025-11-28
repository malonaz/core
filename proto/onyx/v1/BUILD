proto_library(
    name = "v1",
    srcs = [
        "onyx.proto",
        "service.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/onyx/v1",
    },
    root_dir = "proto",
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:validate",
    ],
)
