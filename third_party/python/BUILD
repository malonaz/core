package(
    default_visibility = ["PUBLIC"],
    python = {
        "WHEEL_REPO": "https://get.please.build/third_party/python/py3",
        "WHEEL_NAME_SCHEME": [
            "{url_base}/{package_name}-{version}-${{OS}}_${{ARCH}}.whl",
            "{url_base}/{package_name}-{version}.whl",
            "https://files.pythonhosted.org/packages/py3/{initial}/{package_name}/{package_name}-{version}-py3-none-any.whl",
            "https://files.pythonhosted.org/packages/py2.py3/{initial}/{package_name}/{package_name}-{version}-py2.py3-none-any.whl",
        ],
    },
)

python_wheel(
    name = "protobuf",
    outs = ["google"],
    version = "4.23.1",
)

pip_library(
    name = "grpcio",
    version = "1.55.0",
    licences = ["Apache-2.0"],
    deps = [
        ":six",
    ],
)

pip_library(
    name = "grpclib",
    version = "0.4.4",
    licences = ["BSD-3-Clause"],
    deps = [
        ":h2",
        ":multidict",
        ":protobuf",
    ],
)

python_wheel(
    name = "six",
    outs = ["six.py"],
    hashes = ["8f3cd2e254d8f793e7f3d6d9df77b92252b52637291d0f0da013c76ea2724b6c"],
    version = "1.14.0",
)


pip_library(
    name = "h2",
    version = "4.1.0",
    licences = ["MIT"],
    deps = [":hpack"],
)

pip_library(
    name = "hpack",
    version = "4.0.0",
    licences = ["MIT"],
)

pip_library(
    name = "multidict",
    version = "6.0.4",
    licences = ["Apache-2.0"],
    deps = [":hyperframe"],
)

pip_library(
    name = "hyperframe",
    version = "6.0.1",
    licences = ["MIT"],
)
