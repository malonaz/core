subinclude("//build_defs:proto_extensions")

genrule(
    name = "validate.proto",
    srcs = ["//third_party/go:_github.com__envoyproxy__protoc-gen-validate#download"],
    out = "validate.proto",
    cmd = "mv $SRCS/validate/validate.proto $OUT",
)

proto_library(
    name = "validate",
    srcs = [":validate.proto"],
    visibility = ["PUBLIC"],
)
