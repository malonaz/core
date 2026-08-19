go_library(
    name = "mock_server",
    srcs = [
        "codec.go",
        "health.go",
        "recorder.go",
        "server.go",
    ],
    test_only = True,
    visibility = ["PUBLIC"],
    deps = [
        "//go/grpc/status",
        "//go/mock",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__reflect__protoregistry",
    ],
)

go_test(
    name = "test",
    srcs = ["server_test.go"],
    external = True,
    deps = [
        ":mock_server",
        "//go/grpc/status",
        "//go/test/mock",
        "//malonaz/test/mock/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__credentials__insecure",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)
