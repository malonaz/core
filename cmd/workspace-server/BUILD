subinclude(
    "//build_defs/codegen/onyx:build_defs",
    "//build_defs/proto:descriptor",
)

# Reflection serves this, not the Go registry, whose descriptors drop the comments agents read as tool descriptions.
proto_descriptor_set(
    name = "descriptor_set",
    srcs = ["//malonaz/workspace/workspace_service/v1"],
    deps = ["//malonaz/workspace/workspace_service/v1"],
)

onyx_manifest(
    name = "manifest",
    deps = [
        "//go/workspace/workspace_service:manifest",
        "//malonaz/workspace/workspace_service/v1",
    ],
)

onyx_main_src(
    name = "main.go",
    manifest = ":manifest",
)

go_binary(
    name = "workspace-server",
    srcs = [":main.go"],
    resources = [":descriptor_set"],
    visibility = ["PUBLIC"],
    deps = [
        "//go/certs",
        "//go/flags",
        "//go/grpc",
        "//go/health",
        "//go/logging",
        "//go/prometheus",
        "//go/workspace/workspace_service",
        "//malonaz/workspace/workspace_service/v1",
    ],
)

# Serves a workspace for local agents: plaintext, no authentication, with the reflection the AgentService
# resolves tools through, and no health or metrics listeners to collide with other local servers.
sh_cmd(
    name = "local",
    srcs = [":workspace-server"],
    cmd = " ".join([
        "exec $(out_location :workspace-server)",
        "--logging.format=pretty",
        "--health.disable",
        "--prometheus.disable",
        "--workspace-service-grpc.disable-tls",
        "--workspace-service-grpc.enable-reflection",
        '"$@"',
    ]),
    expand_env_vars = False,
    visibility = ["PUBLIC"],
)
