subinclude("//build_defs/codegen/onyx:build_defs")

onyx_deployment(
    name = "scheduler-service",
    manifest = "manifest.yaml",
    manifest_deps = [
        "//go/scheduler/scheduler_dispatcher:manifest",
        "//go/scheduler/scheduler_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
    test_only = True,
)

go_binary(
    name = "scheduler-service",
    srcs = [":scheduler-service_main.go"],
    test_only = True,
    visibility = ["//go/test/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/lifecycle",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//go/scheduler/scheduler_dispatcher",
        "//go/scheduler/scheduler_service",
        "//go/scheduler/store",
        "//go/sentry",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
)
