subinclude("//build_defs:remote")

# Protoc dependency - referenced in .plzconfig.
protoc_binary(
    name = "protoc",
    version = "21.0",
    visibility = ["PUBLIC"],
)

# Protoc go plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-go",
    binary = True,
    download = "//third_party/go:_google.golang.org__protobuf#download",
    install = ["cmd/protoc-gen-go"],
    module = "google.golang.org/protobuf",
    visibility = ["PUBLIC"],
)

# Gathers the proto go dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-dep",
    exported_deps = [
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__runtime__protoimpl",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__structpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
    ],
    visibility = ["PUBLIC"],
)

# Protoc go grpc plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-go-grpc",
    binary = True,
    module = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
    version = "a82cc96f07c960e02623688e4067ae6b7895334a",
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__pluginpb",
    ],
)

# Gathers the proto go grpc dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-grpc-dep",
    exported_deps = [
        "//third_party/go:google.golang.org__grpc",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
    ],
    visibility = ["PUBLIC"],
)

# Gathers the proto go validate dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-validate-go-dep",
    exported_deps = [
        "//third_party/go:buf.build__gen__go__bufbuild__protovalidate__protocolbuffers__go__buf__validate",
        "//third_party/go:buf.build__gen__go__bufbuild__protovalidate__protocolbuffers__go__buf__validate__priv",
    ],
    visibility = ["PUBLIC"],
)

# Protoc go grpc plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-grpc-gateway",
    binary = True,
    download = "//third_party/go:_github.com__grpc-ecosystem__grpc-gateway__v2#download",
    install = ["protoc-gen-grpc-gateway"],
    module = "github.com/grpc-ecosystem/grpc-gateway/v2",
    visibility = ["PUBLIC"],
    deps = ["//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__protoc-gen-grpc-gateway"],
)

# Gathers the proto go validate depdencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-grpc-gateway-dep",
    exported_deps = [
        "//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__runtime",
        "//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__utilities",
        "//third_party/go:google.golang.org__grpc__grpclog",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
    visibility = ["PUBLIC"],
)

# Python grpc.
git_repo(
    name = "grpc_repo",
    repo = "https://github.com/grpc/grpc",
    revision = "v1.56.0",
    submodules = True,
)

genrule(
    name = "grpc_python_plugin",
    srcs = [":grpc_repo"],
    out = "grpc_python_plugin",
    binary = True,
    cmd = " && ".join([
        "cd $SRCS",
        "cmake CMakeLists.txt",
        "make grpc_python_plugin",
        "cd -",
        "mv $SRCS/grpc_python_plugin .",
    ]),
    visibility = ["PUBLIC"],
)

# GRPC web
GRPCWEB_PROTOC_VERSION = "1.4.2"

remote_file(
    name = "grpc-web-protoc",
    binary = True,
    url = "https://github.com/grpc/grpc-web/releases/download/%s/protoc-gen-grpc-web-%s-$XOS-$XARCH" % (GRPCWEB_PROTOC_VERSION, GRPCWEB_PROTOC_VERSION),
    visibility = ["PUBLIC"],
)
