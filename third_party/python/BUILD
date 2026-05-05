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
    version = "7.34.1",
)

pip_library(
    name = "grpcio",
    version = "1.80.0",
    licences = ["Apache-2.0"],
    deps = [
        ":six",
    ],
)

pip_library(
    name = "grpclib",
    version = "0.4.9",
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
    version = "1.17.0",
)


pip_library(
    name = "h2",
    version = "4.3.0",
    licences = ["MIT"],
    deps = [":hpack"],
)

pip_library(
    name = "hpack",
    version = "4.1.0",
    licences = ["MIT"],
)

pip_library(
    name = "multidict",
    version = "6.7.1",
    licences = ["Apache-2.0"],
    deps = [":hyperframe"],
)

pip_library(
    name = "hyperframe",
    version = "6.1.0",
    licences = ["MIT"],
)
