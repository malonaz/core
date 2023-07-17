go_library(
    name = "jsonnet",
    srcs = [
        "manager.go",
        "parse_once.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/logging",
        "//third_party/go:github.com__fsnotify__fsnotify",
        "//third_party/go:github.com__google__go-jsonnet",
        "//third_party/go:github.com__pkg__errors",
    ],
)

go_test(
    name = "test",
    srcs = ["manager_test.go"],
    deps = [
        ":jsonnet",
        "//third_party/go:github.com__google__uuid",
        "//third_party/go:github.com__nsf__jsondiff",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
