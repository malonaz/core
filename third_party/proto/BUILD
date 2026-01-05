package(default_visibility = ["PUBLIC"])

buf_validate_proto_library(version = "v1.0.0")

GOOGLEAPI_REVISION = "9a477cd3c26a704130e2a2fb44a40281d9312e4c"

googleapis_proto_library(
    name = "google__api",
    dir = "google/api",
    files = ["launch_stage.proto"],
    go_import_suffix = "api",
    revision = GOOGLEAPI_REVISION,
)

googleapis_proto_library(
    name = "google__api__annotations",
    dir = "google/api",
    files = [
        "annotations.proto",
        "client.proto",
        "field_behavior.proto",
        "http.proto",
        "resource.proto",
    ],
    go_import_suffix = "api/annotations",
    revision = GOOGLEAPI_REVISION,
    deps = [":google__api"],
)

googleapis_proto_library(
    name = "google__api__expr__v1alpha1",
    dir = "google/api/expr/v1alpha1",
    files = [
        "checked.proto",
        "eval.proto",
        "explain.proto",
        "syntax.proto",
        "value.proto",
    ],
    go_import_suffix = "api/expr/v1alpha1",
    revision = GOOGLEAPI_REVISION,
    deps = [":google__rpc__status"],
)

googleapis_proto_library(
    name = "google__rpc__status",
    dir = "google/rpc",
    files = ["status.proto"],
    go_import_suffix = "rpc/status",
    revision = GOOGLEAPI_REVISION,
)

googleapis_proto_library(
    name = "google__type__postaladdress",
    dir = "google/type",
    files = ["postal_address.proto"],
    go_import_suffix = "type/postaladdress",
    revision = GOOGLEAPI_REVISION,
)

googleapis_proto_library(
    name = "google__type__date",
    dir = "google/type",
    files = ["date.proto"],
    go_import_suffix = "type/date",
    revision = GOOGLEAPI_REVISION,
)
