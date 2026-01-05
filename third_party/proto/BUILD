package(default_visibility = ["PUBLIC"])

subinclude(
    "//build_defs:remote",
    "///proto//build_defs:proto",
)

GOOGLEAPI_REVISION = "9a477cd3c26a704130e2a2fb44a40281d9312e4c"

github_proto_library(
    name = "validate",
    repo = "bufbuild/protovalidate",
    revision = "v1.0.0",
    dir = "proto/protovalidate/buf/validate",
    files = ["validate.proto"],
    root_dir = "proto/protovalidate",
    additional_context = {GO_IMPORT_PATH: "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"},
)

github_proto_library(
    name = "google__api",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/api",
    files = ["launch_stage.proto"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api"},
)

github_proto_library(
    name = "google__api__annotations",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/api",
    files = [
        "annotations.proto",
        "client.proto",
        "field_behavior.proto",
        "http.proto",
        "resource.proto",
    ],
    deps = [":google__api"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api/annotations"},
)

github_proto_library(
    name = "google__api__expr__v1alpha1",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/api/expr/v1alpha1",
    files = [
        "checked.proto",
        "eval.proto",
        "explain.proto",
        "syntax.proto",
        "value.proto",
    ],
    deps = [":google__rpc__status"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/api/expr/v1alpha1"},
)

github_proto_library(
    name = "google__rpc__status",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/rpc",
    files = ["status.proto"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/rpc/status"},
)

github_proto_library(
    name = "google__type__postaladdress",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/type",
    files = ["postal_address.proto"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/postaladdress"},
)

github_proto_library(
    name = "google__type__date",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
    dir = "google/type",
    files = ["date.proto"],
    additional_context = {GO_IMPORT_PATH: "google.golang.org/genproto/googleapis/type/date"},
)
