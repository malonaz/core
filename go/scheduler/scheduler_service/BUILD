subinclude("//build_defs/codegen/go_service:build_defs")

go_service_manifest(
    name = "manifest",
    visibility = ["PUBLIC"],
)

go_service_src(
    name = "service.tmpl.go",
    labels = ["copy_generated_code::"],
    manifest = ":manifest",
)

go_library(
    name = "scheduler_service",
    srcs = [
        "job.go",
        "metrics.go",
        "queue.go",
        "runtime.go",
        "schema.go",
        "target.go",
        "worker.go",
        ":service.tmpl.go",
    ],
    visibility = ["//..."],
    deps = [
        "//go/aip",
        "//go/grpc",
        "//go/grpc/status",
        "//go/pbutil",
        "//go/pbutil/pbfieldmask",
        "//go/pbutil/pbreflection",
        "//go/routine",
        "//go/scheduler",
        "//go/scheduler/model",
        "//go/scheduler/scheduler_service/rpc",
        "//go/scheduler/store",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:github.com__grpc-ecosystem__go-grpc-middleware__v2__interceptors__retry",
        "//third_party/go:github.com__prometheus__client_golang__prometheus",
        "//third_party/go:github.com__prometheus__client_golang__prometheus__promauto",
        "//third_party/go:google.golang.org__genproto__googleapis__rpc__errdetails",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__metadata",
        "//third_party/go:google.golang.org__grpc__reflection__grpc_reflection_v1",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__reflect__protoreflect",
        "//third_party/go:google.golang.org__protobuf__reflect__protoregistry",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__durationpb",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
        "//third_party/go:google.golang.org__protobuf__types__known__timestamppb",
        "//third_party/proto:google__longrunning__operations",
        "//third_party/proto:google__rpc__code",
        "//third_party/proto:google__rpc__status",
    ],
)
