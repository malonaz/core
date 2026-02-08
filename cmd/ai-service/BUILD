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
    name = "ai-service",
    srcs = [":main.go"],
    deps = [
        "//go/ai/ai_service",
        "//go/authentication",
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/prometheus",
        "//malonaz/ai/ai_service/v1",
    ],
)

sh_cmd(
    name = "local",
    cmd = " ".join([
        "plz run //cmd/ai-service --",
        "--logging.format=pretty",
        "--prometheus.disable",
        # Servers.
        "--ai-service-grpc.disable-tls",
        "--ai-service-grpc.socket-path=/tmp/ai-service.socket",
        "--ai-service-grpc.enable-reflection",
    ]),
)
