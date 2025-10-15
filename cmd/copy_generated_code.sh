#!/usr/bin/env bash
mkdir -p genproto/grpc && cp -rf plz-out/gen/proto/grpc.pb.go genproto/grpc/
mkdir -p genproto/authentication && cp -rf plz-out/gen/proto/authentication.pb.go genproto/authentication/
mkdir -p genproto/codegen/model && cp -rf plz-out/gen/proto/codegen/model.pb.go genproto/codegen/model
mkdir -p genproto/codegen/admin_api && cp -rf plz-out/gen/proto/codegen/admin_api.pb.go genproto/codegen/admin_api
mkdir -p genproto/codegen/rpc && cp -rf plz-out/gen/proto/codegen/rpc.pb.go genproto/codegen/rpc
mkdir -p genproto/codegen/aip && cp -rf plz-out/gen/proto/codegen/aip.pb.go genproto/codegen/aip
