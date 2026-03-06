filegroup(
    name = "proto",
    srcs = [
        "organization.proto",
        "user.proto",
        "user_service.proto",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
)

grpc_library(
    name = "v1",
    srcs = [
        "organization.proto",
        "user.proto",
        "user_service.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/testuser/user_service/v1",
        GENERATE_GO_NATS: True,
    },
    labels = ["copy_generated_code:malonaz:genproto"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/nats/v1",
        "//malonaz/nats/v1",
        "//malonaz/test/user/v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:validate",
    ],
)
