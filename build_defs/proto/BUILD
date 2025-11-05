package(default_visibility = ["PUBLIC"])

subinclude("///proto//build_defs/sdk")

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
    name = "doc",
    srcs = ["doc.build_defs"],
    proto_languages = {
        "proto_language": ["doc_proto_language"],
        "grpc_language": ["doc_proto_language"],
        "grpc_gateway_language": ["doc_proto_language"],
    },
    visibility = ["PUBLIC"],
)

proto_build_defs(
    name = "openapi",
    srcs = ["openapi.build_defs"],
    proto_languages = {
        "grpc_gateway_language": ["openapi_grpc_gateway_language"],
    },
    visibility = ["PUBLIC"],
)

filegroup(
    name = "utils",
    srcs = ["utils.build_defs"],
)

filegroup(
    name = "protoc-gen-doc-template.html",
    srcs = ["protoc-gen-doc-template.html"],
)
