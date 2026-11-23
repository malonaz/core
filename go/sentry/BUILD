go_library(
    name = "sentry",
    srcs = [
        "event.go",
        "sentry.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/grpc/middleware",
        "//go/scheduler",
        "//third_party/go:github.com__getsentry__sentry-go",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__genproto__googleapis__rpc__errdetails",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__status",
    ],
)

go_test(
    name = "test",
    srcs = [
        "event.go",
        "sentry.go",
        "sentry_test.go",
    ],
    deps = [
        "//go/grpc/middleware",
        "//go/grpc/status",
        "//go/scheduler",
        "//third_party/go:github.com__getsentry__sentry-go",
        "//third_party/go:github.com__google__go-cmp__cmp",
        "//third_party/go:github.com__google__go-cmp__cmp__cmpopts",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__genproto__googleapis__rpc__errdetails",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__grpc__status",
    ],
)
