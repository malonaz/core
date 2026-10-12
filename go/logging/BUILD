go_library(
    name = "logging",
    srcs = [
        "context_handler.go",
        "log_fields.go",
        "logging.go",
        "raw_handler.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/logging/pretty",
        "//go/pbutil",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/proto:google__api__annotations",
    ],
)

go_test(
    name = "test",
    srcs = ["log_fields_test.go"],
    deps = [
        ":logging",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
