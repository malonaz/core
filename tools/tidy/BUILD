GODEPS_VERSION = "v0.0.33"

go_module(
    name = "godeps",
    binary = True,
    install = ["cmd/godeps"],
    module = "github.com/sagikazarmark/please-go-modules",
    patch = "godeps.patch",
    version = GODEPS_VERSION,
    deps = [
        "//third_party/go:github.com__bazelbuild__buildtools__build",
        "//third_party/go:github.com__scylladb__go-set__strset",
    ],
)

WOLLEMI_VERSION = "v0.8.1"

remote_file(
    name = "wollemi",
    binary = True,
    exported_files = ["wollemi"],
    extract = True,
    url = f"https://github.com/tcncloud/wollemi/releases/download/{WOLLEMI_VERSION}/wollemi-{WOLLEMI_VERSION}-{CONFIG.HOSTOS}-{CONFIG.HOSTARCH}.tar.gz",
)

BUF_VERSION = "v1.26.1"

genrule(
    name = "buf",
    out = "buf",
    binary = True,
    cmd = [
        f"wget https://github.com/bufbuild/buf/releases/download/{BUF_VERSION}/buf-${{OS^}}-$XARCH -O $OUT",
    ],
    visibility = ["PUBLIC"],
)

def echo_message(message):
    return f'echo "###### {message} ######"'

_lint_cmds = [
    "export GOROOT=\\\\$($(out_exe //tools:go_toolchain|go) env GOROOT)",
    echo_message("Running 'wollemi'"),
    "$(out_exe :wollemi) gofmt ./...",
    echo_message("Running 'gofmt'"),
    f"gofmt -s -w \\\\$(go list -e ./...| sed 's#github.com/[^/]*/[^/]*/\(.*\)#\1#g')",
    echo_message("Running 'buf format'"),
    "$(out_exe :buf) format -w \\\\$(git ls-files | grep '\\.proto' | sed -e 's/^/--path /')",
]

sh_cmd(
    name = "lint",
    cmd = _lint_cmds,
    deps = [
        ":buf",
        ":wollemi",
        "//tools:go_toolchain",
    ],
)

sh_cmd(
    name = "tidy",
    cmd = [
        echo_message("Running 'go mod tidy'"),
        "go mod tidy",
    ] + _lint_cmds,
    deps = [
        ":buf",
        ":godeps",
        ":wollemi",
        "//tools:go_toolchain",
    ],
)
