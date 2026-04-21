filegroup(
    name = "proto",
    srcs = [
        "author.proto",
        "book.proto",
        "library_service.proto",
        "shelf.proto",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = [
        "author.proto",
        "book.proto",
        "library_service.proto",
        "shelf.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/library/library_service/v1",
        GENERATE_GO_NATS: True,
        GENERATE_GO_AIP_LABEL: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/canonicalize/v1",
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/nats/v1",
        "//malonaz/nats/v1",
        "//malonaz/test/library/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
