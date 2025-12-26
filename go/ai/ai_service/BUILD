subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
)

#wollemi:keep
go_library(
    name = "ai_service",
    srcs = [
        "service.go",
        "text_to_text.go",
        "utils.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/ai",
        "//go/ai/ai_service/provider",
        "//go/ai/ai_service/provider/anthropic",
        "//go/ai/ai_service/provider/cartesia",
        "//go/ai/ai_service/provider/elevenlabs",
        "//go/ai/ai_service/provider/openai",
        "//go/grpc",
        "//go/grpc/grpcinproc",
        "//go/pbutil",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//malonaz/audio/v1",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
