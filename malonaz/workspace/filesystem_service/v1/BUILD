filegroup(
    name = "proto",
    srcs = [
        "filesystem.proto",
        "filesystem_service.proto",
        "snapshot.proto",
    ],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = [
        "filesystem.proto",
        "filesystem_service.proto",
        "snapshot.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/workspace/filesystem_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/workspace/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
