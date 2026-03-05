go_library(
    name = "health",
    srcs = [
        "grpc_server.go",
        "health.go",
        "http_server.go",
        "metrics.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/grpc/status",
        "//go/pbutil",
        "//third_party/go:github.com__prometheus__client_golang__prometheus",
        "//third_party/go:github.com__prometheus__client_golang__prometheus__promauto",
        "//third_party/go:golang.org__x__sync__errgroup",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__health",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
    ],
)
