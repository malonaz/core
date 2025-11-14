#!/usr/bin/env bash
rm -rf gen
# Proto libraries
mkdir -p genproto/grpc/v1 && cp -rf plz-out/gen/proto/grpc/v1/grpc.pb.go genproto/grpc/v1
mkdir -p genproto/authentication/v1 && cp -rf plz-out/gen/proto/authentication/v1/authentication.pb.go genproto/authentication/v1
mkdir -p genproto/scheduler/v1 && cp -rf plz-out/gen/proto/scheduler/v1/job.pb.go genproto/scheduler/v1
mkdir -p genproto/codegen/model/v1 && cp -rf plz-out/gen/proto/codegen/model/v1/model.pb.go genproto/codegen/model/v1
mkdir -p genproto/codegen/gateway/v1 && cp -rf plz-out/gen/proto/codegen/gateway/v1/gateway.pb.go genproto/codegen/gateway/v1
mkdir -p genproto/codegen/aip/v1 && cp -rf plz-out/gen/proto/codegen/aip/v1/aip.pb.go genproto/codegen/aip/v1
# Test files.
mkdir -p genproto/test/aip && cp -rf plz-out/gen/proto/test/aip/aip.pb.go genproto/test/aip

# Go files.
mkdir -p genproto/scheduler/v1/model && sed 's/"proto\//"github.com\/malonaz\/core\/genproto\//g' plz-out/gen/proto/scheduler/v1/job.model.go > genproto/scheduler/v1/model/job.model.go

# Regenerate the BUILD files.
plz lint
