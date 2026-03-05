proto_library(
    name = "v1",
    srcs = ["event.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/codegen/nats/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/nats/v1",
    ],
)
