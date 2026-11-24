subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "library-service",
    test_only = True,
    deps = [
        "//go/test/library/library_service:manifest",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/test/library/library_service/v1",
    ],
)

go_binary(
    name = "library-service",
    srcs = [":library-service_main.go"],
    test_only = True,
    visibility = ["//go/test/library/library_service/..."],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/nats",
        "//go/postgres",
        "//go/pprof",
        "//go/prometheus",
        "//go/scheduler/longrunning",
        "//go/sentry",
        "//go/test/library/library_service",
        "//go/test/library/store",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/test/library/library_service/v1",
        "//third_party/proto:google__longrunning__operations",
    ],
)
