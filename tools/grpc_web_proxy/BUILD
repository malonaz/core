subinclude("//build_defs:distroless")

GRPC_WEB_PROXY_VERSION = "0.15.0"

remote_file(
    name = "grpcwebproxy_zip",
    out = "grpcwebproxy.zip",
    url = "https://github.com/improbable-eng/grpc-web/releases/download/v%s/grpcwebproxy-v%s-$XOS-$XARCH.zip" % (GRPC_WEB_PROXY_VERSION, GRPC_WEB_PROXY_VERSION),
)

genrule(
    name = "grpc_web_proxy",
    srcs = [":grpcwebproxy_zip"],
    outs = ["grpcwebproxy"],
    binary = True,
    cmd = "$TOOL x $SRCS && mv dist/* grpcwebproxy",
    tools = [CONFIG.JARCAT_TOOL],
)

distroless_go(
    name = "docker_image",
    binary = ":grpcwebproxy",
    visibility = ["PUBLIC"],
)
