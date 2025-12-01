grpc_gateway_library(
    name = "gateway",
    srcs = ["gateway.proto"],
    additional_context = {
        GO_IMPORT_PATH: "github.com/malonaz/core/genproto/test/gateway",
    },
    test_only = True,
    deps = [
        "//third_party/proto:google__api__annotations",
    ],
)
