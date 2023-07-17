go_binary(
    name = "grpc_health_probe",
    srcs = ["main.go"],
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/go:github.com__spiffe__go-spiffe__v2__spiffetls__tlsconfig",
        "//third_party/go:github.com__spiffe__go-spiffe__v2__workloadapi",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__credentials",
        "//third_party/go:google.golang.org__grpc__health__grpc_health_v1",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__grpc__status",
    ],
)
