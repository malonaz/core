subinclude("//build_defs/codegen/protoc_gen_core:build_defs")

go_protoc_gen_core(
    name = "codegen",
    srcs = ["//malonaz/workspace/v1:proto"],
    additional_context = {
        GO_PROTOC_GEN_CORE_GO_IMPORT_PATHS: {
            "model": "//gengo/workspace/model",
        },
    },
    labels = ["copy_generated_code:go:gengo"],
    plugin = "postgres",
    deps = ["//malonaz/workspace/v1"],
)

go_library(
    name = "store",
    srcs = [":codegen"],
    import_path = "github.com/malonaz/core/gengo/workspace/store",
    visibility = ["PUBLIC"],
    deps = [
        "//go/postgres",
        "//go/workspace/model",
        "//third_party/go:github.com__jackc__pgx__v5",
    ],
)

go_test(
    name = "store_test",
    srcs = ["store_test.go"],
    data = ["//go/workspace/migrations"],
    deps = [
        ":store",
        "//go/postgres",
        "//go/postgres/test_server",
        "//go/uuid",
        "//go/workspace/model",
        "//malonaz/workspace/v1",
        "//third_party/go:github.com__google__go-cmp__cmp",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__protobuf__testing__protocmp",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
