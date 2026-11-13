filegroup(
    name = "proto",
    srcs = [
        "agent.proto",
        "memory.proto",
        "task.proto",
    ],
    visibility = ["PUBLIC"],
)

proto_library(
    name = "v1",
    srcs = [
        "agent.proto",
        "memory.proto",
        "task.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/agent/v1",
        GENERATE_GO_AIP: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/model/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
