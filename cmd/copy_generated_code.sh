#!/usr/bin/env bash
rm -rf genproto
mkdir -p genproto/grpc && cp -rf plz-out/gen/proto/grpc/grpc.pb.go genproto/grpc/
mkdir -p genproto/authentication && cp -rf plz-out/gen/proto/authentication/authentication.pb.go genproto/authentication/
mkdir -p genproto/codegen/model && cp -rf plz-out/gen/proto/codegen/model/model.pb.go genproto/codegen/model
mkdir -p genproto/codegen/gateway && cp -rf plz-out/gen/proto/codegen/gateway/gateway.pb.go genproto/codegen/gateway
mkdir -p genproto/codegen/aip && cp -rf plz-out/gen/proto/codegen/aip/aip.pb.go genproto/codegen/aip

# Test files.
mkdir -p genproto/test/aip && cp -rf plz-out/gen/proto/test/aip/aip.pb.go genproto/test/aip

# Regenerate the BUILD files.
plz lint
