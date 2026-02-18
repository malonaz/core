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

# JS RULES.
genrule(
    name = "protoc-gen-es",
    srcs = ["protoc-gen-es.json"],
    outs = ["protoc-gen-es"],
    building_description = "fetching node modules for protoc-gen-es",
    cmd = "mv $SRCS $PKG/package.json && yarn --cwd $PKG_DIR --cache-folder $PWD/.yarn-cache install && mkdir protoc-gen-es && mv $PKG_DIR/node_modules protoc-gen-es",
    output_is_complete = True,
    visibility = ["PUBLIC"],
)

genrule(
    name = "protoc-gen-connect-query",
    srcs = ["protoc-gen-connect-query.json"],
    outs = ["protoc-gen-connect-query"],
    building_description = "fetching node modules for protoc-gen-connect-query",
    cmd = "mv $SRCS $PKG/package.json && yarn --cwd $PKG_DIR --cache-folder $PWD/.yarn-cache install && mkdir protoc-gen-connect-query && mv $PKG_DIR/node_modules protoc-gen-connect-query",
    output_is_complete = True,
    visibility = ["PUBLIC"],
)

# Protoc ruby grpc plugin - referenced in .plzconfig.
genrule(
    name = "protoc-gen-ruby-grpc",
    outs = ["protoc-gen-ruby-grpc"],
    cmd = [
        "GEM_HOME=$PWD/protoc-gen-ruby-grpc gem install grpc-tools --no-document",
        "rm -rf protoc-gen-ruby-grpc/cache",
    ],
    output_is_complete = True,
    visibility = ["PUBLIC"],
)
