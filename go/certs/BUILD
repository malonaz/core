go_library(
    name = "certs",
    srcs = ["certs.go"],
    visibility = ["PUBLIC"],
)

go_test(
    name = "test",
    srcs = ["certs_test.go"],
    deps = [
        ":certs",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
