go_library(
    name = "schema",
    srcs = [
        "descendants.go",
        "schema.go",
        "search.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/aip/transpiler/postgres/static",
        "//go/pbutil",
        "//malonaz/codegen/aip/v1",
        "//malonaz/codegen/model/v1",
        "//third_party/go:github.com__huandu__xstrings",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__reflect__protoregistry",
        "//third_party/proto:google__api__annotations",
        "//tools/protoc-gen-core/protofield",
        "//tools/protoc-gen-core/resource",
    ],
)

go_test(
    name = "test",
    srcs = [
        "aggregate_join_test.go",
        "correlated_filter_test.go",
    ],
    external = True,
    deps = [
        ":schema",
        "//malonaz/codegen/model/v1",
        "//malonaz/test/library/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protodesc",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__reflect__protoregistry",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__pluginpb",
        "//third_party/proto:google__api__annotations",
        "//third_party/proto:google__type__decimal",
        "//tools/protoc-gen-core/resource",
    ],
)

go_test(
    name = "internal_test",
    srcs = ["name_expr_test.go"],
    deps = [
        ":schema",
        "//third_party/go:github.com__stretchr__testify__require",
        "//tools/protoc-gen-core/resource",
    ],
)
