go_library(
    name = "pod",
    srcs = [
        "pods.go",
        "spec.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/grpc",
        "//malonaz/workspace/v1",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/go:k8s.io__api__core__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__api__errors",
        "//third_party/go:k8s.io__apimachinery__pkg__api__resource",
        "//third_party/go:k8s.io__apimachinery__pkg__apis__meta__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__fields",
        "//third_party/go:k8s.io__apimachinery__pkg__types",
        "//third_party/go:k8s.io__apimachinery__pkg__util__intstr",
        "//third_party/go:k8s.io__apimachinery__pkg__watch",
        "//third_party/go:k8s.io__client-go__kubernetes",
        "//third_party/go:k8s.io__client-go__kubernetes__typed__core__v1",
    ],
)

go_test(
    name = "test",
    srcs = [
        "pods_test.go",
        "spec_test.go",
    ],
    deps = [
        ":pod",
        "//go/grpc",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/go:k8s.io__api__core__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__api__resource",
        "//third_party/go:k8s.io__apimachinery__pkg__apis__meta__v1",
        "//third_party/go:k8s.io__apimachinery__pkg__util__intstr",
        "//third_party/go:k8s.io__client-go__kubernetes__fake",
    ],
)
