go_library(
    name = "resourcename",
    srcs = [
        "ancestor.go",
        "containswildcard.go",
        "doc.go",
        "hasparent.go",
        "isdomainname.go",
        "join.go",
        "matches.go",
        "rangeparents.go",
        "scanner.go",
        "segment.go",
        "sprint.go",
        "sscan.go",
        "validate.go",
        "wildcard.go",
    ],
    visibility = ["PUBLIC"],
)

go_test(
    name = "test",
    srcs = [
        "ancestor_test.go",
        "containswildcard_test.go",
        "hasparent_test.go",
        "join_test.go",
        "matches_test.go",
        "rangeparents_test.go",
        "scanner_test.go",
        "sprint_test.go",
        "sscan_test.go",
        "validate_test.go",
    ],
    deps = [
        ":resourcename",
        "//third_party/go:github.com__stretchr__testify__require",
    ],
)
