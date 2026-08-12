proto_library(
    name = "v1",
    srcs = [
        "component.proto",
        "content.proto",
        "interaction.proto",
        "interaction_response.proto",
        "resource.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/ai/genui/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
