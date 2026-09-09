filegroup(
    name = "proto",
    srcs = [
        "configuration.proto",
        "job.proto",
        "scheduler_service.proto",
    ],
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = [
        "configuration.proto",
        "job.proto",
        "scheduler_service.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/scheduler/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
