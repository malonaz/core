go_library(
    name = "scheduler",
    srcs = [
        "policy.go",
        "scheduler.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/grpc/status",
        "//go/uuid",
        "//malonaz/scheduler/policy/v1",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/proto:google__rpc__code",
    ],
)

go_test(
    name = "scheduler_test",
    srcs = ["policy_test.go"],
    deps = [
        ":scheduler",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
