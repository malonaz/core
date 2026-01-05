package(default_visibility = ["PUBLIC"])

######################### BUF ########################
github_proto_library(
    name = "validate",
    dir = "proto/protovalidate",
    root_dir = "proto/protovalidate",
    files = [
        "buf/validate/validate.proto",
    ],
    additional_context = {
        GO_IMPORT_PATH: "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate",
    },
    repo = "bufbuild/protovalidate",
    revision = "v1.0.0",
)

######################### GOOGLEAPIS ########################
GOOGLEAPI_REVISION = "5e5d500bc4f8c4d5591cd1fda817556b3fe663a7"

github_proto_library(
    name = "google__api__annotations",
    dir = "google/api",
    files = [
        "annotations.proto",
        "client.proto",
        "field_behavior.proto",
        "http.proto",
        "resource.proto",
    ],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__api__expr__v1alpha1",
    dir = "google/api/expr/v1alpha1",
    files = [
        "checked.proto",
        "eval.proto",
        "explain.proto",
        "syntax.proto",
        "value.proto",
    ],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    deps = [
        ":google__rpc__status",
    ],
)

github_proto_library(
    name = "google__rpc__status",
    dir = "google/rpc",
    files = ["status.proto"],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__type__postaladdress",
    dir = "google/type",
    files = ["postal_address.proto"],
    repo = "googleapis/googleapis",
    additional_context = {
        GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/postaladdress",
    },
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__type__date",
    dir = "google/type",
    files = ["date.proto"],
    repo = "googleapis/googleapis",
    additional_context = {
        GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/date",
    },
    revision = GOOGLEAPI_REVISION,
)
