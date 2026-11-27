subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "filesystem-manager",
    deps = [
        "//go/workspace/filesystem_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/workspace/filesystem_service/v1",
    ],
)

go_binary(
    name = "filesystem-manager",
    srcs = [":filesystem-manager_main.go"],
    visibility = ["PUBLIC"],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/pprof",
        "//go/prometheus",
        "//go/scheduler/longrunning",
        "//go/sentry",
        "//go/workspace/filesystem_service",
        "//go/workspace/store",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/workspace/filesystem_service/v1",
        "//third_party/proto:google__longrunning__operations",
    ],
)
