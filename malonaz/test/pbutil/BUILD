proto_library(
    name = "pbutil",
    srcs = ["pbutil.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/pbutil",
    },
    labels = ["copy_generated_code"],
    test_only = True,
    visibility = ["//go/pbutil/..."],
    deps = [
        "//third_party/proto:google__api__annotations",
    ],
)
