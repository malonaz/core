subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
)

go_service_src(
    name = "service.tmpl.go",
    manifest = ":manifest",
)

go_library(
    name = "ai_service",
    srcs = [
        "runtime.go",
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
        "//go/ai/ai_service/provider/deepgram",
        "//go/ai/ai_service/provider/elevenlabs",
        "//go/ai/ai_service/provider/google",
        "//go/ai/ai_service/provider/openai",
        "//go/ai/ai_service/provider/xai",
        "//go/ai/ai_service/rpc",
        "//go/ai/store",
        "//go/grpc",
        "//go/grpc/grpcinproc",
        "//go/pbutil",
        "//go/pbutil/pbfieldmask",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//malonaz/audio/v1",
        "//third_party/go:golang.org__x__sync__errgroup",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
