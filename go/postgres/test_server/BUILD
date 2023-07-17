go_library(
    name = "test_server",
    srcs = [
        "server.go",
        "utils.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/binary",
        "//go/logging",
        "//go/postgres",
        "//go/postgres/migrator",
        "//go/postgres/migrator/migrations",
        "//third_party/go:github.com__pkg__errors",
    ],
)
