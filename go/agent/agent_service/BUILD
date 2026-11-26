subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
    deps = [
        "//malonaz/ai/ai_service/v1",
    ],
)

go_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "agent_service",
    srcs = [
        "agent.go",
        "agent_loop.go",
        "compaction.go",
        "helpers.go",
        "queue.go",
        "runtime.go",
        "task_loop.go",
        "terminal.go",
        "tools.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/agent/agent_service/rpc",
        "//go/agent/model",
        "//go/agent/store",
        "//go/aip",
        "//go/grpc/status",
        "//go/postgres",
        "//go/uuid",
        "//malonaz/agent/agent_service/v1",
        "//malonaz/agent/v1",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/ai/v1",
        "//malonaz/json/v1",
        "//third_party/go:github.com__jackc__pgx__v5",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
