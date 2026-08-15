go_test(
    name = "sat",
    srcs = [
        "engine_test.go",
        "sat_test.go",
    ],
    data = [
        "//cmd/ai-engine",
    ],
    test_outputs = ["sat.log"],
    deps = [
        "//go/ai/tool",
        "//go/aip",
        "//go/grpc",
        "//go/grpc/require",
        "//go/prometheus",
        "//go/sat",
        "//malonaz/ai/ai_engine/v1",
        "//malonaz/ai/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
    ],
)
