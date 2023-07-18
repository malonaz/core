go_library(
    name = "mock_server",
    srcs = [
        "codec.go",
        "handler.go",
        "server.go",
    ],
    test_only = True,
    deps = [
        "//common/go/certs",
        "//common/go/grpc",
        "//common/go/logging",
        "//common/go/prometheus",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)
