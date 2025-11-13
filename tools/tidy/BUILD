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

BUF_VERSION = "v1.59.0"

genrule(
    name = "buf",
    out = "buf",
    binary = True,
    cmd = [
        f'''
        OS_NAME={CONFIG.OS}
        ARCH_FINAL=$(if [ "$ARCH" == "amd64" ]; then echo $XARCH; else echo $ARCH; fi)
        curl -L https://github.com/bufbuild/buf/releases/download/{BUF_VERSION}/buf-$OS_NAME-$ARCH_FINAL -o $OUT
        ''',
    ],
    visibility = ["PUBLIC"],
)

def echo_message(message):
    return f'echo "###### {message} ######"'

_lint_cmds = [
    "export GOROOT=\\\\$($(out_exe //tools/go:toolchain|go) env GOROOT)",
    echo_message("Building codegen"),
    "plz build --include codegen > /dev/null 2>&1 || echo 'Codegen build failed'",
    echo_message("Running 'wollemi'"),
    "mv third_party/go/dummy/dummy.go third_party/go/dummy/dummy.ignore 2>/dev/null || true",
    "$(out_exe :wollemi) gofmt ./...",
    "mv third_party/go/dummy/dummy.ignore third_party/go/dummy/dummy.go 2>/dev/null || true",
    echo_message("Running 'gofmt'"),
    f"git ls-files | grep '\.go$'| xargs $(out_exe //tools/go:toolchain|gofmt) -l -s -w",
    echo_message("Running 'buf format'"),
    f"$(out_exe :buf) format -w \\\\$(git ls-files | grep '\\.proto' | sed -e 's/^/--path /') --exclude-path plz-out/ --exclude-path .plz-cache --config $(out_location {CONFIG.CORE.BUF_CONFIG})",
    echo_message("Running 'buf lint'"),
    f"$(out_exe :buf) lint --exclude-path plz-out/ --exclude-path .plz-cache --config $(out_location {CONFIG.CORE.BUF_CONFIG}) \\\\$(git ls-files | grep '\\.proto' | sed -e 's/^/--path /')",
]

sh_cmd(
    name = "lint",
    cmd = _lint_cmds,
    deps = [
        ":buf",
        CONFIG.CORE.BUF_CONFIG,
        ":wollemi",
        "//tools/go:toolchain",
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
        CONFIG.CORE.BUF_CONFIG,
        ":godeps",
        ":wollemi",
        "//tools/go:toolchain",
    ],
)

export_file(
    name = "buf.yaml",
    src = "buf.yaml",
)
