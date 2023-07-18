go_library(
    name = "migrator",
    srcs = [
        "migrator.go",
        "sql_queries.go",
    ],
    visibility = ["//..."],
    deps = [
        "//common/go/logging",
        "//common/go/postgres",
        "//common/go/postgres/migrator/migrations",
        "//third_party/go:github.com__pkg__errors",
    ],
)
