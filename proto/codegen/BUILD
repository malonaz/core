package(default_visibility = ["PUBLIC"])

proto_library(
    name = "model",
    srcs = ["model.proto"],
)

proto_library(
    name = "gateway",
    srcs = ["gateway.proto"],
)

proto_library(
    name = "rpc",
    srcs = ["rpc.proto"],
)

proto_library(
    name = "aip",
    srcs = ["aip.proto"],
    deps = [
        "//third_party/proto:googleapis_v1alpha1",
    ],
)
