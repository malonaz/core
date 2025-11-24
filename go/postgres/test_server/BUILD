go_library(
    name = "test_server",
    srcs = [
        "server.go",
        "utils.go",
    ],
    test_only = True,
    visibility = ["//..."],
    deps = [
        "//go/binary",
        "//go/postgres",
        "//go/postgres/migrator",
        "//go/postgres/migrator/migrations",
    ],
)
