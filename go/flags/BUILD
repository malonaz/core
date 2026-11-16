go_library(
    name = "flags",
    srcs = ["flags.go"],
    visibility = ["PUBLIC"],
    deps = ["//third_party/go:github.com__jessevdk__go-flags"],
)

go_test(
    name = "test",
    srcs = ["flags_test.go"],
    deps = [
        ":flags",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
