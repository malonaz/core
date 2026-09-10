go_library(
    name = "longrunning",
    srcs = [
        "operation.go",
        "runner.go",
        "server.go",
    ],
    visibility = ["PUBLIC"],
    deps = [
        "//go/aip",
        "//go/grpc/status",
        "//go/scheduler",
        "//go/scheduler/model",
        "//malonaz/scheduler/scheduler_service/v1",
        "//malonaz/scheduler/v1",
        "//third_party/go:go.einride.tech__aip__resourcename",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__grpc__status",
        "//third_party/go:google.golang.org__protobuf__proto",
        "//third_party/go:google.golang.org__protobuf__types__known__anypb",
        "//third_party/go:google.golang.org__protobuf__types__known__emptypb",
        "//third_party/proto:google__longrunning__operations",
    ],
)
