package(default_visibility = ["PUBLIC"])

subinclude(
    "//build_defs:remote",
    "///proto//build_defs:proto",
)

GOOGLEAPI_REVISION = "9a477cd3c26a704130e2a2fb44a40281d9312e4c"

github_proto_library(
    name = "validate",
    additional_context = {GO_IMPORT_PATH: "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"},
    dir = "proto/protovalidate/buf/validate",
    files = ["validate.proto"],
    repo = "bufbuild/protovalidate",
    revision = "v1.0.0",
    root_dir = "proto/protovalidate",
)

github_proto_library(
    name = "google__api",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api"},
    dir = "google/api",
    files = ["launch_stage.proto"],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__api__annotations",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api/annotations"},
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
    deps = [":google__api"],
)

github_proto_library(
    name = "google__api__expr__v1alpha1",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api/expr/v1alpha1"},
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
    deps = [":google__rpc__status"],
)

github_proto_library(
    name = "google__rpc__status",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/rpc/status"},
    dir = "google/rpc",
    files = ["status.proto"],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__type__postaladdress",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/postaladdress"},
    dir = "google/type",
    files = ["postal_address.proto"],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

github_proto_library(
    name = "google__type__date",
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/date"},
    dir = "google/type",
    files = ["date.proto"],
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)
