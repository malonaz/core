go_library(
    name = "test_server",
    srcs = [
        "server.go",
        "utils.go",
    ],
    visibility = ["//..."],
    deps = [
        "//common/go/binary",
        "//common/go/logging",
        "//common/go/postgres",
        "//common/go/postgres/migrator",
        "//common/go/postgres/migrator/migrations",
        "//third_party/go:github.com__pkg__errors",
    ],
)
