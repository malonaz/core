filegroup(
    name = "proto",
    srcs = ["processor.proto"],
    test_only = True,
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["processor.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/scheduler/processor/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = ["//third_party/proto:google__longrunning__operations"],
)
