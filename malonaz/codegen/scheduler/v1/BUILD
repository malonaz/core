proto_library(
    name = "v1",
    srcs = ["scheduler.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/codegen/scheduler/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/scheduler/policy/v1",
        "//third_party/proto:validate",
    ],
)
