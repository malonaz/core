subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "scheduler-service",
    test_only = True,
    deps = [
        "//go/scheduler/scheduler_dispatcher:manifest",
        "//go/scheduler/scheduler_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
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
        "//go/pprof",
        "//go/prometheus",
        "//go/scheduler/scheduler_dispatcher",
        "//go/scheduler/scheduler_service",
        "//go/scheduler/store",
        "//go/sentry",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
)
