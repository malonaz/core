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
    name = "server",
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
        "plz run //server --",
        "--logging.format=pretty",
        "--prometheus.disable",
        # Servers.
        "--core-grpc.disable-tls",
        "--core-grpc.socket-path=/tmp/core.socket",
        "--core-grpc.enable-reflection",
        "--external-api-key-authentication.config=authentication/external_service_accounts.json",
        "--internal-service-authentication.config=authentication/internal_service_accounts.json",
        "--permission-authentication.config=authentication/roles.json",
    ]),
)
