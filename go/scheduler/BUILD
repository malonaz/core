go_library(
    name = "scheduler",
    srcs = [
        "policy.go",
        "scheduler.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/scheduler/policy/v1",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/proto:google__rpc__code",
    ],
)
