go_test(
    name = "sat",
    srcs = [
        "author_test.go",
        "book_test.go",
        "sat_test.go",
        "shelf_test.go",
    ],
    data = [
        "//cmd/library-service",
        "//go/library/library_service",
        "//go/library/migrations:initializer",
        "//go/library/migrations:migrator",
    ],
    deps = [
        "//go/grpc",
        "//go/grpc/middleware",
        "//go/grpc/require",
        "//go/prometheus",
        "//go/sat",
        "//go/uuid",
        "//malonaz/library/library_service/v1",
        "//malonaz/library/v1",
        "//third_party/go:github.com__stretchr__testify__require",
        "//third_party/go:google.golang.org__grpc__codes",
        "//third_party/go:google.golang.org__protobuf__types__known__fieldmaskpb",
    ],
)
