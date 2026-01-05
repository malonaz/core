package(default_visibility = ["PUBLIC"])

# Protoc dependency - referenced in .plzconfig.
protoc_binary(
    name = "protoc",
    version = "32.1",
)

# Protoc go plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-go",
    binary = True,
    download = "//third_party/go:_google.golang.org__protobuf#download",
    install = ["cmd/protoc-gen-go"],
    module = "google.golang.org/protobuf",
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
)

# Protoc go grpc plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-go-grpc",
    binary = True,
    module = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
    version = "a82cc96f07c960e02623688e4067ae6b7895334a",
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
)

# Protoc go validate plugin - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-validate-go-dep",
    exported_deps = [
        "//third_party/go:buf.build__gen__go__bufbuild__protovalidate__protocolbuffers__go__buf__validate",
    ],
)

# Protoc go grpc plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-grpc-gateway",
    binary = True,
    download = "//third_party/go:_github.com__grpc-ecosystem__grpc-gateway__v2#download",
    install = ["protoc-gen-grpc-gateway"],
    module = "github.com/grpc-ecosystem/grpc-gateway/v2",
    deps = ["//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__protoc-gen-grpc-gateway"],
)

# Gathers the proto go validate depdencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-grpc-gateway-dep",
    exported_deps = [
        "//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__runtime",
        "//third_party/go:github.com__grpc-ecosystem__grpc-gateway__v2__utilities",
        "//third_party/go:google.golang.org__grpc__grpclog",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__protobuf__proto",
    ],
)

# Protoc go aip plugin - referenced in .plzconfig.
go_module(
    name = "protoc-gen-go-aip",
    binary = True,
    download = "//third_party/go:_go.einride.tech__aip#download",
    install = ["cmd/protoc-gen-go-aip"],
    module = "go.einride.tech/aip",
    deps = [
        "//third_party/go:github.com__stoewer__go-strcase",
        "//third_party/go:go.einride.tech__aip__reflect__aipreflect",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__genproto__googleapis__api__annotations",
        "//third_party/go:google.golang.org__protobuf__compiler__protogen",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__reflect__protoregistry",
    ],
)

# Gathers the proto go aip dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-aip-dep",
    exported_deps = [
        "//third_party/go:go.einride.tech__aip__resourcename",
    ],
)

# Gathers the proto go model dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-model-dep",
    exported_deps = [
    ],
)

# Gathers the proto go llm dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-llm-dep",
    exported_deps = [
    ],
)

# Gathers the proto go rpc dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-rpc-dep",
    exported_deps = [
    ],
)

# Gathers the proto go postgres dependencies - referenced in .plzconfig.
filegroup(
    name = "protoc-gen-go-postgres-dep",
    exported_deps = [
    ],
)
