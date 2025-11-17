package(default_visibility = ["PUBLIC"])

######################### BUF ########################
external_github_proto_library(
    name = "validate",
    dir = "buf/validate",
    files = [
        "proto/protovalidate/buf/validate/validate.proto",
    ],
    go_dep = "//third_party/go:buf.build__gen__go__bufbuild__protovalidate__protocolbuffers__go__buf__validate",
    repo = "bufbuild/protovalidate",
    revision = "v1.0.0",
)

######################### GOOGLEAPIS ########################
GOOGLEAPI_REVISION = "5e5d500bc4f8c4d5591cd1fda817556b3fe663a7"

external_github_proto_library(
    name = "google__api__annotations",
    files = [
        "google/api/annotations.proto",
        "google/api/client.proto",
        "google/api/field_behavior.proto",
        "google/api/http.proto",
        "google/api/resource.proto",
    ],
    go_dep = "//third_party/go:google.golang.org__genproto__googleapis__api__annotations",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

external_github_proto_library(
    name = "google__api__expr__v1alpha1",
    files = [
        "google/api/expr/v1alpha1/checked.proto",
        "google/api/expr/v1alpha1/eval.proto",
        "google/api/expr/v1alpha1/explain.proto",
        "google/api/expr/v1alpha1/syntax.proto",
        "google/api/expr/v1alpha1/value.proto",
    ],
    go_dep = "//third_party/go:google.golang.org__genproto__googleapis__api__expr__v1alpha1",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

external_github_proto_library(
    name = "google__rpc__status",
    files = ["google/rpc/status.proto"],
    go_dep = "//third_party/go:google.golang.org__genproto__googleapis__rpc__status",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

external_github_proto_library(
    name = "google__type__postaladdress",
    files = ["google/type/postal_address.proto"],
    go_dep = "//third_party/go:google.golang.org__genproto__googleapis__type__postaladdress",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)

external_github_proto_library(
    name = "google__type__date",
    files = ["google/type/date.proto"],
    go_dep = "//third_party/go:google.golang.org__genproto__googleapis__type__date",
    repo = "googleapis/googleapis",
    revision = GOOGLEAPI_REVISION,
)
