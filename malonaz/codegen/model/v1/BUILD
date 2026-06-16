proto_library(
    name = "v1",
    srcs = ["model.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/codegen/model/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:google__api__annotations",
    ],
)
