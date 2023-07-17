go_library(
    name = "postgres",
    srcs = [
        "client.go",
        "insert.go",
        "select.go",
        "types.go",
        "utils.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/logging",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgxpool",
        "//third_party/go:github.com__pkg__errors",
    ],
)

go_test(
    name = "test",
    srcs = [
        "insert_test.go",
        "select_test.go",
        "types_test.go",
        "utils_test.go",
    ],
    deps = [
        ":postgres",
        "//third_party/go:github.com__lib__pq",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
