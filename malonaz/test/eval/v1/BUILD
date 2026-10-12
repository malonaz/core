proto_library(
    name = "v1",
    srcs = ["evals.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/eval/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["//go/ai/eval/..."],
    deps = ["//malonaz/ai/eval/v1"],
)
