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

go_module(
    name = "wollemi",
    binary = True,
    module = "github.com/tcncloud/wollemi",
    patch = "wollemi.patch",
    version = "v0.8.1",
    deps = [
        "//third_party/go:github.com__bazelbuild__buildtools__build",
        "//third_party/go:github.com__please-build__gcfg",
        "//third_party/go:github.com__sirupsen__logrus",
        "//third_party/go:github.com__spf13__cobra",
        "//third_party/go:golang.org__x__mod__modfile",
    ],
)

BUF_VERSION = "v1.26.1"

genrule(
    name = "buf",
    out = "buf",
    binary = True,
    cmd = [
        f'wget https://github.com/bufbuild/buf/releases/download/{BUF_VERSION}/buf-${OS^}-$(if [ "$ARCH" == "amd64" ]; then echo $XARCH; else echo $ARCH; fi) -O $OUT',
    ],
    visibility = ["PUBLIC"],
)

def echo_message(message):
    return f'echo "###### {message} ######"'

_lint_cmds = [
    "export GOROOT=\\\\$($(out_exe //tools:go_toolchain|go) env GOROOT)",
    echo_message("Building codegen"),
    "plz build --include codegen > /dev/null 2>&1 || echo 'Codegen build failed'",
    echo_message("Running 'wollemi'"),
    "$(out_exe :wollemi) gofmt ./...",
    echo_message("Running 'gofmt'"),
    f"git ls-files | grep '\.go$'| xargs gofmt -l -s -w",
    echo_message("Running 'buf format'"),
    "$(out_exe :buf) format -w \\\\$(git ls-files | grep '\\.proto' | sed -e 's/^/--path /') --exclude-path plz-out/",
    echo_message("Removing third_party/go/dummy/BUILD.plz"),
    "rm -f $(git rev-parse --show-toplevel)/third_party/go/dummy/BUILD.plz",
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
        echo_message("Running 'go deps'"),
        "$(out_exe :godeps) -dir third_party/go -builtin -wollemi -arm",
    ] + _lint_cmds,
    deps = [
        ":buf",
        ":godeps",
        ":wollemi",
        "//tools:go_toolchain",
    ],
)
