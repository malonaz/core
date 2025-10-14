package(default_visibility = ["PUBLIC"])

PROTOVALIDATE_REVISION = "adbfb457c64b2b2009ed699c98940691428c7ab3"

github_proto_library(
    name = "validate",
    additional_context = {
        "go_import_path": "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate",
    },
    dir = "proto/protovalidate/buf/validate",
    files = [
        "validate.proto",
    ],
    protoc_flags = ["-I" + package_name() + "/proto/protovalidate"],
    repo = "bufbuild/protovalidate",
    revision = PROTOVALIDATE_REVISION,
)
