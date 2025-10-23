package(default_visibility = ["PUBLIC"])

subinclude("///proto//build_defs/sdk")

proto_build_defs(
    name = "go",
    srcs = ["go.build_defs"],
    proto_languages = {
        "grpc_language": ["go_grpc_language"],
        "proto_language": ["go_proto_language"],
    },
    visibility = ["PUBLIC"],
)

filegroup(
    name = "utils",
    srcs = ["utils.build_defs"],
)
