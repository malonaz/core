go_library(
    name = "analysis",
    srcs = [
        "analyses.go",
        "chat.go",
        "transcript.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/analysis",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
    ],
)

go_test(
    name = "test",
    srcs = ["analysis_test.go"],
    deps = [
        ":analysis",
        "//go/ai/ai_service/provider/mock",
        "//go/ai/analysis/fixtures",
        "//go/analysis",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//malonaz/analysis/v1",
        "//third_party/go:github.com__google__go-cmp__cmp",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__testing__protocmp",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)

# Manual: calls a real decision model, so needs TYPESAFE_API_KEY and costs money.
go_test(
    name = "eval",
    srcs = [
        "analysis_test.go",
        "eval_test.go",
    ],
    labels = ["manual"],
    sandbox = False,
    deps = [
        ":analysis",
        "//go/ai/ai_service/provider",
        "//go/ai/ai_service/provider/mock",
        "//go/ai/ai_service/provider/typesafe",
        "//go/ai/analysis/fixtures",
        "//go/analysis",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//malonaz/analysis/v1",
        "//third_party/go:github.com__google__go-cmp__cmp",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__testing__protocmp",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
