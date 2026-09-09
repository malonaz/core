subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    test_only = True,
    deps = [
        "//go/scheduler/scheduler_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
)

go_main_src(
    name = "main.go",
    manifest = ":manifest",
    test_only = True,
)

go_binary(
    name = "scheduler-service",
    srcs = [":main.go"],
    test_only = True,
    visibility = ["//go/test/scheduler/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//go/scheduler/scheduler_service",
        "//go/scheduler/store",
        "//malonaz/scheduler/scheduler_service/v1",
    ],
)
