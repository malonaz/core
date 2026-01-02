proto_library(
    name = "v1",
    srcs = [
        "schema.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/json/v1",
    },
    labels = ["copy_generated_code"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:validate",
    ],
)
