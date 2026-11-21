go_library(
    name = "histogram",
    srcs = ["histogram.go"],
    visibility = ["//..."],
)

go_test(
    name = "histogram_test",
    srcs = ["histogram_test.go"],
    deps = [
        ":histogram",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
