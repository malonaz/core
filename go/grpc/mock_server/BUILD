go_library(
    name = "mock_server",
    srcs = [
        "codec.go",
        "handler.go",
        "server.go",
    ],
    test_only = True,
    deps = [
        "//go/certs",
        "//go/grpc",
        "//go/logging",
        "//go/pbutil",
        "//go/prometheus",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)
