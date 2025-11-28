proto_library(
    name = "aip",
    srcs = ["aip.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/aip",
    },
    root_dir = "proto",
    test_only = True,
    visibility = ["//go/aip/..."],
    deps = [
        "//genproto/codegen/aip/v1",
        "//genproto/codegen/model/v1",
        "//proto/codegen/aip/v1",
        "//proto/codegen/model/v1",
    ],
)
