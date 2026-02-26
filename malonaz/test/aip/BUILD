proto_library(
    name = "aip",
    srcs = ["aip.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/aip",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["//go/aip/..."],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/model/v1",
        "//third_party/proto:google__api__annotations",
    ],
)
