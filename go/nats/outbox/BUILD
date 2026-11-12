go_library(
    name = "outbox",
    srcs = [
        "outbox.go",
        "poller.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/nats",
        "//go/postgres",
        "//go/routine",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
        "//third_party/go:github.com__prometheus__client_golang__prometheus",
        "//third_party/go:github.com__prometheus__client_golang__prometheus__promauto",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

go_test(
    name = "outbox_test",
    srcs = [
        "outbox_test.go",
        "poller_test.go",
    ],
    data = ["//go/nats/outbox/migrations"],
    deps = [
        ":outbox",
        "//go/nats",
        "//go/postgres",
        "//go/postgres/test_server",
        "//malonaz/nats/v1",
        "//third_party/go:github.com__nats-io__nats-server__v2__test",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__protobuf__types__known__wrapperspb",
    ],
)
