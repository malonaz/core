go_library(
    name = "postgres",
    srcs = [
        "client.go",
        "insert.go",
        "select.go",
        "types.go",
        "utils.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/logging",
        "//third_party/go:github.com__jackc__pgerrcode",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
        "//third_party/go:github.com__jackc__pgx__v5__pgxpool",
    ],
)

go_test(
    name = "postgres_test",
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
