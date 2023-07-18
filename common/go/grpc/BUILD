subinclude("//build_defs:proto_extensions")

go_library(
    name = "grpc",
    srcs = [
        "client.go",
        "cookie.go",
        "gateway.go",
        "opts.go",
        "server.go",
        "utils.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        ":types",
        "//common/go/certs",
        "//common/go/health",
        "//common/go/logging",
        "//common/go/prometheus",
        "//third_party/go:github.com__bufbuild__protovalidate-go",
        "//third_party/go:github.com__grpc-ecosystem__go-grpc-middleware",
        "//third_party/go:github.com__grpc-ecosystem__go-grpc-middleware__retry",
        "//third_party/go:github.com__grpc-ecosystem__go-grpc-prometheus",
        "//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__runtime",
        "//third_party/go:github.com__pkg__errors",
        "//third_party/go:github.com__sercand__kuberesolver__v5",
        "//third_party/go:golang.org__x__net__context",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__balancer__roundrobin",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__credentials",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
        "//third_party/go:google.golang.org__grpc__keepalive",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__encoding__protojson",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

proto_library(
    name = "types",
    srcs = ["types.proto"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/proto/buf:validate",
    ],
)

go_test(
    name = "types_test",
    srcs = ["types_test.go"],
    external = True,
    deps = [
        ":types",
        "//third_party/go:github.com__bufbuild__protovalidate-go",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
