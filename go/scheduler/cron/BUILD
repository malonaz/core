go_library(
    name = "cron",
    srcs = ["cron.go"],
    visibility = ["//..."],
    deps = ["//third_party/go:github.com__robfig__cron__v3"],
)

go_test(
    name = "cron_test",
    srcs = ["cron_test.go"],
    deps = [
        ":cron",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
