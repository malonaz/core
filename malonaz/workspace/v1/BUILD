filegroup(
    name = "proto",
    srcs = [
        "filesystem.proto",
        "labels.proto",
        "snapshot.proto",
        "workspace.proto",
    ],
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "filesystem.proto",
        "labels.proto",
        "snapshot.proto",
        "workspace.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/workspace/v1",
        GENERATE_GO_AIP: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
