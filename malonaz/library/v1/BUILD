filegroup(
    name = "proto",
    srcs = [
        "author.proto",
        "book.proto",
        "shelf.proto",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "author.proto",
        "book.proto",
        "shelf.proto",
    ],
    additional_context = {
        GENERATE_GO_AIP: True,
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/library/v1",
    },
    labels = ["copy_generated_code"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/canonicalize/v1",
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/model/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
