subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "workspace-manager",
    deps = [
        "//go/workspace/workspace_manager_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/workspace/filesystem_service/v1",
        "//malonaz/workspace/workspace_manager_service/v1",
    ],
)

go_binary(
    name = "workspace-manager",
    srcs = [":workspace-manager_main.go"],
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
        "//go/workspace/store",
        "//go/workspace/workspace_manager_service",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/workspace/filesystem_service/v1",
        "//malonaz/workspace/workspace_manager_service/v1",
        "//third_party/proto:google__longrunning__operations",
    ],
)
