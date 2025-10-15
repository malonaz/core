#!/usr/bin/env bash
mkdir -p genproto/grpc && cp -rf plz-out/gen/proto/grpc.pb.go genproto/grpc/
mkdir -p genproto/authentication && cp -rf plz-out/gen/proto/authentication.pb.go genproto/authentication/
