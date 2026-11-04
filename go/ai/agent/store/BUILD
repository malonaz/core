subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/ai/agent/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/ai/agent/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    deps = ["//malonaz/ai/agent/v1"],
)

filegroup(
    name = "handwritten",
    srcs = [
        "agent.go",
        "event.go",
        "executor.go",
        "input.go",
        "transaction.go",
    ],
    labels = [
        "codegen",
        "copy_generated_code:go:gengo",
    ],
)

go_library(
    name = "store",
    srcs = [
        "agent.go",
        "event.go",
        "executor.go",
        "input.go",
        "transaction.go",
        ":codegen",
    ],
    import_path = "github.com/malonaz/core/gengo/ai/agent/store",
    visibility = ["PUBLIC"],
    deps = [
        "//go/ai/agent/model",
        "//go/aip",
        "//go/pbutil",
        "//go/postgres",
        "//go/uuid",
        "//malonaz/ai/agent/v1",
        "//third_party/go:github.com__jackc__pgerrcode",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:github.com__jackc__pgx__v5__pgconn",
    ],
)
