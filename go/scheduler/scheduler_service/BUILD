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
    name = "scheduler_service",
    srcs = [
        "job.go",
        "maintenance.go",
        "metrics.go",
        "queue.go",
        "runtime.go",
        "schedule.go",
        "tick.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/grpc/status",
        "//go/pbutil/pbfieldmask",
        "//go/routine",
        "//go/scheduler",
        "//go/scheduler/cron",
        "//go/scheduler/model",
        "//go/scheduler/scheduler_service/rpc",
        "//go/scheduler/store",
        "//go/scheduler/transition",
        "//go/uuid",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:buf.build__go__protovalidate",
        "//third_party/go:github.com__prometheus__client_golang__prometheus",
        "//third_party/go:github.com__prometheus__client_golang__prometheus__promauto",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
)
