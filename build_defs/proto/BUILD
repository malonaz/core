package(default_visibility = ["PUBLIC"])

subinclude("///proto//build_defs/sdk")

filegroup(
    name = "descriptor",
    srcs = ["descriptor.build_defs"],
)

filegroup(
    name = "utils",
    srcs = ["utils.build_defs"],
)

proto_build_defs(
    name = "go",
    srcs = ["go.build_defs"],
    proto_languages = {
        "grpc_language": ["go_grpc_language"],
        "proto_language": ["go_proto_language"],
        "grpc_gateway_language": ["go_grpc_gateway_language"],
    },
    visibility = ["PUBLIC"],
)

proto_build_defs(
    name = "js",
    srcs = ["js.build_defs"],
    proto_languages = {
        "grpc_language": ["js_grpc_language"],
        "proto_language": ["js_proto_language"],
        "grpc_gateway_language": ["js_grpc_gateway_language"],
    },
    visibility = ["PUBLIC"],
)

proto_build_defs(
    name = "ruby",
    srcs = ["ruby.build_defs"],
    proto_languages = {
        "grpc_language": ["ruby_grpc_language"],
        "proto_language": ["ruby_proto_language"],
        "grpc_gateway_language": ["ruby_grpc_gateway_language"],
    },
    visibility = ["PUBLIC"],
)
