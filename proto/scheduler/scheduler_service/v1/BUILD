subinclude("//build_defs/codegen/go_proto_templates:build_defs")

subinclude("///core//build_defs/codegen/go_service:build_defs")

grpc_library(
    name = "v1",
    srcs = ["scheduler_service.proto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto:core__codegen__aip__v1",
        "//third_party/proto:core__codegen__model__v1",
        "//third_party/proto:core__scheduler__v1",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:google__api__expr__v1alpha1",
        "//third_party/proto:validate",
    ],
)

go_proto_template_library(
    name = "rpc",
    srcs = ["scheduler_service.proto"],
    template = "//build_defs/codegen/go_proto_templates/templates:rpc",
    visibility = ["PUBLIC"],
    deps = [":v1"],
)

go_service_manifest(
    name = "manifest",
    schema = "//build_defs/codegen/go_service/schemas:service.go.schema",
    visibility = ["PUBLIC"],
)

go_service_src(
    name = "service.tmpl.go",
    exclude_default_templates = True,
    manifest = ":manifest",
    templates = [
        "//build_defs/codegen/go_service/templates:service.go.tmpl",
        "//build_defs/codegen/go_service/templates:utils.go.tmpl",
    ],
)
