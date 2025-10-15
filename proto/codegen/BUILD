package(default_visibility = ["PUBLIC"])

proto_library(
    name = "model",
    srcs = ["model.proto"],
)

proto_library(
    name = "admin_api",
    srcs = ["admin_api.proto"],
)

proto_library(
    name = "rpc",
    srcs = ["rpc.proto"],
)

proto_library(
    name = "aip",
    srcs = ["aip.proto"],
    deps = [
        "//third_party/proto/googleapis:v1alpha1",
    ],
)
