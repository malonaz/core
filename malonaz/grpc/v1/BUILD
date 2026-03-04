proto_library(
    name = "v1",
    srcs = [
        "extensions.proto",
        "gateway.proto",
        "hook.proto",
        "http_cookie.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/grpc/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
)
