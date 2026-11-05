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
        "edit_file.go",
        "exec.go",
        "read_files.go",
        "runtime.go",
        "write_file.go",
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
        "edit_file_test.go",
        "exec_test.go",
        "read_files_test.go",
        "runtime_test.go",
        "write_file_test.go",
    ],
    deps = [
        ":workspace_service",
        "//go/grpc/require",
        "//malonaz/workspace/workspace_service/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc__codes",
    ],
)
