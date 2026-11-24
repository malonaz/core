grpc_library(
    name = "v2",
    srcs = [
        "authentication.proto",
        "token_service.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/authentication/v2",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:validate",
    ],
)
