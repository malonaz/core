proto_library(
    name = "v1",
    srcs = [
        "aip.proto",
        "annotation.proto",
        "extensions.proto",
        "import.proto",
        "label.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/aip/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:google__rpc__status",
        "//third_party/proto:validate",
    ],
)
