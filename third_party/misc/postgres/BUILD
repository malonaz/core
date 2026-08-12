config_setting(
    name = "_linux_amd64",
    values = {
        "os": "linux",
        "cpu": "amd64",
    },
)

config_setting(
    name = "_linux_arm64",
    values = {
        "os": "linux",
        "cpu": "arm64",
    },
)

config_setting(
    name = "_darwin_amd64",
    values = {
        "os": "darwin",
        "cpu": "amd64",
    },
)

config_setting(
    name = "_darwin_arm64",
    values = {
        "os": "darwin",
        "cpu": "arm64",
    },
)

# Relocatable PostgreSQL builds, so system acceptance tests do not depend on a postgres
# installed on the host. Tracks the major version used in development and production.
_VERSION = "16.8.0"

_BASE_URL = "https://github.com/theseus-rs/postgresql-binaries/releases/download"

remote_file(
    name = "postgres-archive",
    extract = True,
    url = select({
        ":_linux_amd64": f"{_BASE_URL}/{_VERSION}/postgresql-{_VERSION}-x86_64-unknown-linux-gnu.tar.gz",
        ":_linux_arm64": f"{_BASE_URL}/{_VERSION}/postgresql-{_VERSION}-aarch64-unknown-linux-gnu.tar.gz",
        ":_darwin_amd64": f"{_BASE_URL}/{_VERSION}/postgresql-{_VERSION}-x86_64-apple-darwin.tar.gz",
        ":_darwin_arm64": f"{_BASE_URL}/{_VERSION}/postgresql-{_VERSION}-aarch64-apple-darwin.tar.gz",
    }),
)

# The whole tree is kept intact: postgres resolves share/extension and lib/ relative to the
# real path of its own binary, and core's test server derives initdb and pg_ctl from
# whichever directory `postgres` resolves to on PATH.
genrule(
    name = "postgres",
    srcs = [":postgres-archive"],
    outs = ["postgres"],
    cmd = "mv $(dirname $(dirname $(find $SRCS -type f -name postgres -path '*/bin/*' | head -1))) $OUTS",
    visibility = ["PUBLIC"],
)
