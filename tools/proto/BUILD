# Protoc dependency - referenced in .plzconfig.
protoc_binary(
    name = "protoc",
    version = "32.1",
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
        "//third_party/go:google.golang.org__genproto__googleapis__api__annotations",
        "//third_party/go:google.golang.org__genproto__googleapis__api__expr__v1alpha1",
        "//third_party/go:google.golang.org__genproto__googleapis__rpc__status",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__runtime__protoimpl",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
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

# Protoc go validate plugin - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-validate-go-dep",
    exported_deps = [
        "//third_party/go:buf.build__gen__go__bufbuild__protovalidate__protocolbuffers__go__buf__validate",
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

# Protoc gen cobra plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-cobra",
    binary = True,
    download = "//third_party/go:_github.com__NathanBaulch__protoc-gen-cobra#download",
    module = "github.com/NathanBaulch/protoc-gen-cobra",
    visibility = ["PUBLIC"],
    deps = [
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__pluginpb",
    ],
)

# Gathers the proto gen cobra dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-cobra-dep",
    exported_deps = [
        "//third_party/go:github.com__NathanBaulch__protoc-gen-cobra__client",
        "//third_party/go:github.com__NathanBaulch__protoc-gen-cobra__flag",
        "//third_party/go:github.com__NathanBaulch__protoc-gen-cobra__iocodec",
        "//third_party/go:github.com__spf13__cobra",
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__types__descriptorpb",
        "//third_party/go:google.golang.org__protobuf__types__pluginpb",
    ],
    visibility = ["PUBLIC"],
)
