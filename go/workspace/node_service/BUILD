subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
)

onyx_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "node_service",
    srcs = [
        "copies.go",
        "csi.go",
        "filesystems.go",
        "objects.go",
        "runtime.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/grpc",
        "//go/grpc/status",
        "//go/health",
        "//go/pbutil",
        "//go/uuid",
        "//malonaz/workspace/node_service/v1",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__aws__aws-sdk-go-v2__aws",
        "//third_party/go:github.com__aws__aws-sdk-go-v2__config",
        "//third_party/go:github.com__aws__aws-sdk-go-v2__service__s3",
        "//third_party/go:github.com__aws__aws-sdk-go-v2__service__s3__types",
        "//third_party/go:github.com__container-storage-interface__spec__lib__go__csi",
        "//third_party/go:github.com__klauspost__compress__zstd",
        "//third_party/go:golang.org__x__sys__unix",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__wrapperspb",
    ],
)

go_test(
    name = "test",
    srcs = ["node_service_test.go"],
    deps = [
        ":node_service",
        "//go/grpc/require",
        "//go/uuid",
        "//malonaz/workspace/node_service/v1",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__container-storage-interface__spec__lib__go__csi",
        "//third_party/go:github.com__klauspost__compress__zstd",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc__codes",
    ],
)
