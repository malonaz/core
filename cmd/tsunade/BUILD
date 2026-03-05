subinclude("//build_defs/codegen/go_service:build_defs")

go_main_manifest(
    name = "manifest",
    deps = [
        "//go/ai/ai_service:manifest",
        "//malonaz/ai/ai_service/v1",
    ],
)

go_main_src(
    name = "main.go",
    manifest = ":manifest",
)

go_binary(
    name = "tsunade",
    srcs = [
        "init.go",
        ":main.go",
    ],
    deps = [
        "//go/ai/ai_service",
        "//go/ai/store",
        "//go/authentication",
        "//go/binary",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/postgres",
        "//go/prometheus",
        "//malonaz/ai/ai_service/v1",
    ],
)

sh_binary(
    name = "local",
    main = "local.sh",
)
