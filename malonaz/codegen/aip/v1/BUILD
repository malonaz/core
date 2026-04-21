proto_library(
    name = "v1",
    srcs = [
        "aip.proto",
        "label.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/codegen/aip/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/aip/v1",
        "//third_party/proto:validate",
    ],
)
