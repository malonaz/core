filegroup(
    name = "proto",
    srcs = ["mock_service.proto"],
    test_only = True,
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = ["mock_service.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/mock/v1",
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["PUBLIC"],
)
