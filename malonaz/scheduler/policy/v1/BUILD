proto_library(
    name = "v1",
    srcs = ["policy.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/scheduler/policy/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:google__rpc__code",
        "//third_party/proto:validate",
    ],
)
