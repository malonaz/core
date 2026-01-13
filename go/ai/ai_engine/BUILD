subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/ai/ai_service/v1",
        "//third_party/proto:grpc__reflection__v1",
    ],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
)

go_library(
    name = "ai_engine",
    srcs = [
        "service.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/ai",
        "//go/grpc",
        "//go/pbutil",
        "//go/pbutil/pbjson",
        "//go/pbutil/pbreflection",
        "//malonaz/ai/ai_engine/v1",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__reflection__grpc_reflection_v1",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
    ],
)
