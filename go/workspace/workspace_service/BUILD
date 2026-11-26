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
    name = "workspace_service",
    srcs = [
        "exec.go",
        "runtime.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/grpc/status",
        "//malonaz/workspace/workspace_service/v1",
        "//third_party/go:google.golang.org__grpc__codes",
    ],
)

go_test(
    name = "test",
    srcs = [
        "exec_test.go",
        "runtime_test.go",
    ],
    deps = [
        ":workspace_service",
        "//go/grpc/require",
        "//malonaz/workspace/workspace_service/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc__codes",
    ],
)
