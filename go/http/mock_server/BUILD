go_library(
    name = "mock_server",
    srcs = [
        "recorder.go",
        "server.go",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//go/mock",
        "//go/pbutil",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

go_test(
    name = "test",
    srcs = ["server_test.go"],
    external = True,
    deps = [
        ":mock_server",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
