subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/scheduler/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/scheduler/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    deps = ["//malonaz/scheduler/v1"],
)

filegroup(
    name = "handwritten",
    srcs = [
        "claim.go",
        "job.go",
        "queue.go",
        "schedule.go",
    ],
    labels = [
        "codegen",
        "copy_generated_code:go:gengo",
    ],
)

go_library(
    name = "store",
    srcs = [
        "claim.go",
        "job.go",
        "queue.go",
        "schedule.go",
        ":codegen",
    ],
    import_path = "github.com/malonaz/core/gengo/scheduler/store",
    visibility = ["//..."],
    deps = [
        "//go/postgres",
        "//go/scheduler/model",
        "//malonaz/scheduler/v1",
        "//third_party/go:github.com__jackc__pgerrcode",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
    ],
)
