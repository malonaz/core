filegroup(
    name = "proto",
    srcs = [
        "job.proto",
        "labels.proto",
        "queue.proto",
        "target.proto",
    ],
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "job.proto",
        "labels.proto",
        "queue.proto",
        "target.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/scheduler/v1",
        GENERATE_GO_AIP: True,
        GENERATE_GO_AIP_LABEL: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/model/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:google__rpc__code",
        "//third_party/proto:google__rpc__status",
        "//third_party/proto:validate",
    ],
)
