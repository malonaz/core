go_library(
    name = "sat",
    srcs = [
        "environment.go",
        "files.go",
        "sat.go",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//go/binary",
        "//go/logging",
        "//go/postgres/test_server",
        "//third_party/go:github.com__nats-io__nats-server__v2__server",
        "//third_party/go:github.com__nats-io__nats-server__v2__test",
    ],
)
