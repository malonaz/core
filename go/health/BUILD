go_library(
    name = "health",
    srcs = [
        "grpc_server.go",
        "health.go",
        "http_server.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/logging",
        "//go/pbutil",
        "//third_party/go:golang.org__x__sync__errgroup",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__health",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
        "//third_party/go:google.golang.org__grpc__status",
    ],
)
