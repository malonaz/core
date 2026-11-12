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
        "//go/uuid",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
        "//third_party/go:github.com__nats-io__nats.go__jetstream",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

go_test(
    name = "outbox_test",
    srcs = ["poller_test.go"],
    deps = [
        ":outbox",
        "//go/nats",
        "//malonaz/nats/v1",
        "//third_party/go:github.com__nats-io__nats-server__v2__test",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
