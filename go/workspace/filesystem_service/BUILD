subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
    deps = ["//malonaz/scheduler/scheduler_service/v1"],
)

onyx_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "filesystem_service",
    srcs = [
        "attachments.go",
        "create_snapshot.go",
        "delete_filesystem.go",
        "nodes.go",
        "runtime.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/grpc",
        "//go/grpc/status",
        "//go/scheduler",
        "//go/uuid",
        "//go/workspace/filesystem_service/rpc",
        "//go/workspace/model",
        "//go/workspace/store",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/workspace/filesystem_service/v1",
        "//malonaz/workspace/node_service/v1",
        "//malonaz/workspace/v1",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/go:k8s.io__api__core__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__apis__meta__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__fields",
        "//third_party/go:k8s.io__apimachinery__pkg__labels",
        "//third_party/go:k8s.io__client-go__kubernetes",
        "//third_party/go:k8s.io__client-go__kubernetes__typed__core__v1",
        "//third_party/go:k8s.io__client-go__rest",
    ],
)

go_test(
    name = "test",
    srcs = ["filesystem_service_test.go"],
    data = ["//go/workspace/migrations"],
    deps = [
        ":filesystem_service",
        "//go/grpc/require",
        "//go/postgres",
        "//go/postgres/test_server",
        "//go/uuid",
        "//go/workspace/model",
        "//go/workspace/store",
        "//malonaz/workspace/filesystem_service/v1",
        "//malonaz/workspace/node_service/v1",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__google__go-cmp__cmp",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__testing__protocmp",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
