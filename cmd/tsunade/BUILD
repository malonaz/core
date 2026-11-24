subinclude("//build_defs/codegen/onyx:build_defs")

onyx_manifest(
    name = "manifest",
    binary = "tsunade",
    deps = [
        "//go/ai/ai_service:manifest",
        "//malonaz/ai/ai_service/v1",
    ],
)

go_binary(
    name = "tsunade",
    srcs = [
        "init.go",
        ":tsunade_main.go",
    ],
    deps = [
        "//go/ai/ai_service",
        "//go/ai/store",
        "//go/authentication/v2",
        "//go/binary",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/pprof",
        "//go/prometheus",
        "//go/sentry",
        "//malonaz/ai/ai_service/v1",
        "//malonaz/authentication/v2",
        "//third_party/go:google.golang.org__grpc",
    ],
)

sh_binary(
    name = "local",
    main = "local.sh",
)

go_test(
    name = "test",
    srcs = ["authentication_test.go"],
    data = ["authentication/authentication.jsonnet"],
    external = True,
    deps = [
        "//go/authentication/v2",
        "//go/grpc",
        "//malonaz/ai/ai_service/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__credentials",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__grpc__status",
    ],
)
