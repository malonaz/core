go_library(
    name = "outbox",
    srcs = [
        "outbox.go",
        "relay.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/postgres",
        "//go/routine",
        "//go/scheduler",
        "//go/uuid",
        "//malonaz/aip/v1",
        "//malonaz/scheduler/scheduler_service/v1",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

go_test(
    name = "outbox_test",
    srcs = ["relay_test.go"],
    deps = [
        ":outbox",
        "//malonaz/aip/v1",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
    ],
)
