subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
    deps = ["//malonaz/ai/ai_service/v1"],
)

onyx_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "agent_service",
    srcs = [
        "broker.go",
        "executor.go",
        "lease.go",
        "placement.go",
        "report.go",
        "runtime.go",
        "stream.go",
        "sweeper.go",
        "txn.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/ai",
        "//go/ai/agent/agent_service/rpc",
        "//go/ai/agent/model",
        "//go/ai/agent/store",
        "//go/aip",
        "//go/grpc/status",
        "//go/uuid",
        "//malonaz/ai/agent/agent_service/v1",
        "//malonaz/ai/agent/v1",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/proto:google__rpc__status",
    ],
)

go_test(
    name = "test",
    srcs = [
        "broker_test.go",
        "service_test.go",
        "store_test.go",
        "stream_test.go",
    ],
    data = ["//go/ai/agent/migrations"],
    deps = [
        ":agent_service",
        "//go/ai/agent/model",
        "//go/ai/agent/store",
        "//go/aip",
        "//go/grpc/grpcinproc",
        "//go/grpc/status",
        "//go/postgres",
        "//go/postgres/test_server",
        "//go/uuid",
        "//malonaz/ai/agent/agent_service/v1",
        "//malonaz/ai/agent/v1",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
