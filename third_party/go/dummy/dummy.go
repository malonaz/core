/*
This file is used to list dependencies we want to pull in manually.
Dependencies used in go code are automatically pulled, but some dependencies used by arbitrary build defs (think codegen) aren't so we add them here.
*/
package dummy

import (
	_ "github.com/bazelbuild/buildtools/build"
	_ "github.com/envoyproxy/protoc-gen-validate"
	_ "github.com/google/subcommands"
	_ "github.com/google/wire"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway"
	_ "github.com/kevinburke/go-bindata/v4"
	_ "github.com/scylladb/go-set/strset"
	_ "golang.org/x/tools/go/packages"
	_ "golang.org/x/tools/go/types/typeutil"
	_ "golang.org/x/tools/imports"
	_ "google.golang.org/protobuf/compiler/protogen"
	_ "google.golang.org/protobuf/types/pluginpb"
)
