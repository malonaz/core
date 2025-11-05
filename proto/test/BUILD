proto_library(
    name = "aip",
    srcs = ["aip.proto"],
    test_only = True,
    visibility = ["//go/aip/..."],
    deps = [
        "//genproto/codegen/aip",
        "//proto/codegen:aip",
    ],
)

grpc_gateway_library(
    name = "gateway",
    srcs = ["gateway.proto"],
    test_only = True,
    deps = [
        "//third_party/proto:googleapis_annotations",
    ],
)
