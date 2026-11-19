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

# crane edits images in the registry without a Docker daemon; crane_image uses it to append layers.
_VERSION = "v0.22.1"

_BASE_URL = f"https://github.com/google/go-containerregistry/releases/download/{_VERSION}"

remote_file(
    name = "crane-archive",
    extract = True,
    hashes = [
        "0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0",
        "898c0cff975f898a33e8c4580bdafb0e7c02c7faa33374e946762f97c4ab7110",
        "6fedd06a648c11335f0e8b9547e4783002c30e9336047fec292942cd518bb799",
        "2231fc8df8806d20d680ff1225db44e095a55dd6ac1ae8eced4faf4b278b78fb",
    ],
    url = select({
        ":_linux_amd64": f"{_BASE_URL}/go-containerregistry_Linux_x86_64.tar.gz",
        ":_linux_arm64": f"{_BASE_URL}/go-containerregistry_Linux_arm64.tar.gz",
        ":_darwin_amd64": f"{_BASE_URL}/go-containerregistry_Darwin_x86_64.tar.gz",
        ":_darwin_arm64": f"{_BASE_URL}/go-containerregistry_Darwin_arm64.tar.gz",
    }),
)

genrule(
    name = "crane",
    srcs = [":crane-archive"],
    outs = ["crane"],
    binary = True,
    cmd = "mv $(find $SRCS -type f -name crane | head -1) $OUTS",
    visibility = ["PUBLIC"],
)
