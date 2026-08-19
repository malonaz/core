go_library(
    name = "test_store",
    srcs = ["nats.go"],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//go/nats",
        "//malonaz/aip/v1",
        "//third_party/go:github.com__nats-io__nats.go",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

go_test(
    name = "test",
    srcs = ["nats_test.go"],
    external = True,
    deps = [
        ":test_store",
        "//go/nats",
        "//malonaz/aip/v1",
        "//third_party/go:github.com__nats-io__nats-server__v2__test",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)
