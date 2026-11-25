subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
    deps = ["//malonaz/ai/ai_service/v1"],
)

onyx_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "ai_processor",
    srcs = [
        "mirror.go",
        "runtime.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/ai/tool",
        "//go/aip",
        "//go/grpc",
        "//go/pbutil/pbfieldmask",
        "//go/pbutil/pbreflection",
        "//go/routine",
        "//go/uuid",
        "//malonaz/ai/ai_engine/v1",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:github.com__prometheus__client_golang__prometheus",
        "//third_party/go:github.com__prometheus__client_golang__prometheus__promauto",
        "//third_party/go:google.golang.org__grpc__reflection__grpc_reflection_v1",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
    ],
)
